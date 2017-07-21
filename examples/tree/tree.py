import hpx
import sys
import numpy as np

# define types
hpx_addr_type = int
particle_type = np.dtype([('pos', float), ('mass', float), ('phi', float)])
moment_type = np.dtype([('mtot', float), ('xcom', float), ('Q00', float)])
node_type = np.dtype([('left', hpx_addr_type), ('right', hpx_addr_type), ('low', float), ('high', float), 
                      ('moments', moment_type), ('parts', hpx_addr_type), ('count', int)])

@hpx.create_action()
def main(n_parts, n_partition, theta_c, domain_size):
    set_domain_size(hpx.NULL(), domain_size, sync='rsync')
    root = create_node(0.0, domain_size)
    parts = generate_parts(n_parts, domain_size, root.addr)

    done = hpx.Future(shape=(1,), dtype=moment_type)
    partition_node(root[0], root[0], parts, n_parts, n_partition, sync='lsync', rsync_lco=done)
    done.wait()
    done.delete()

    alldone = hpx.And(n_parts)
    spawn_computation(root[0], root[0].addr.addr, root, alldone, theta_c)
    alldone.wait()
    alldone.delete()

    hpx.exit()

@hpx.create_action()
def set_domain_size(domain_size_arg):
    global domain_size, domain_count
    domain_size = domain_size_arg
    domain_count = hpx.get_num_ranks()
    return hpx.SUCCESS

def compute_moments(parts, n_parts):
    moment = np.array([(0.0, 0.0, 0.0)], dtype=moment_type)
    moment['mtot'] = np.sum(parts['mass'])
    moment['xcom'] = np.sum(parts['mass']*parts['pos'])
    if moment['mtot'] > 0:
        moment['xcom'] /= moment['mtot']
        dx = parts['pos'] - moment['xcom']
        moment['Q00'] = np.sum(2*parts['mass']*dx*dx)
    return moment

@hpx.create_id_action(moment_type)
def moment_reduction_id(array):
    array['mtot'] = 0
    array['xcom'] = 0
    array['Q00'] = 0

@hpx.create_op_action(moment_type)
def moment_reduction_op(lhs, rhs):
    newtot = lhs['mtot'] + rhs['mtot']
    newcom = 0.0
    newQ00 = 0.0

    if newtot > 0.0:
        newcom = lhs['mtot'] * lhs['xcom'] + rhs['mtot'] * rhs['xcom']
        newcom /= newtot
        dxl = lhs['xcom'] - newcom
        newQ00 = 2.0 * lhs['mtot'] * dxl * dxl + lhs['Q00']
        dxr = rhs['xcom'] - newcom
        newQ00 += 2.0 * rhs['mtot'] * dxr * dxr + rhs['Q00'] 

    lhs['mtot'] = newtot
    lhs['xcom'] = newcom
    lhs['Q00'] = newQ00

@hpx.create_action()
def partition_node(node_gas, parts, n_parts, n_partition):
    node = node_gas.try_pin()
    node['parts'] = parts.addr.addr
    node['count'] = n_parts

    if n_parts <= n_partition:
        parts_local = parts[0].try_pin()
        node['moments'] = compute_moments(parts_local, n_parts)
        parts[0].unpin()
        hpx.thread_continue('array', node['moments'])
    else:
        parts_local = parts[0].try_pin()
        split = 0.5*(node[0]['low'] + node[0]['high'])
        parts_local_left = parts_local[parts_local['pos'] < split]
        parts_local_right = parts_local[parts_local['pos'] >= split]
        parts[0].unpin()

        reduce_lco = hpx.Reduce(2, (1,), moment_type, moment_reduction_id, moment_reduction_op)
        
        if parts_local_left.shape[0] > 0:
            node_left = create_node(node[0]['low'], split)
            node['left'] = node_left.addr.addr
            parts_left = hpx.GlobalMemory.alloc_local_at(1, parts_local_left.shape[0], particle_type, node['left'])
            cpy_done = hpx.Future()
            parts_left[0].set(parts_local_left, sync='lsync', rsync_lco=cpy_done)
            cpy_done.wait()
            cpy_done.delete()
            partition_node(node_left[0], node_left[0], parts_left, parts_local_left.shape[0], n_partition, rsync_lco=reduce_lco)
        else:
            empty = np.zeros(1, dtype=moment_type)
            reduce_lco.set(array=empty)

        if parts_local_right.shape[0] > 0:
            node_right = create_node(split, node[0]['high'])
            node['right'] = node_right.addr.addr
            parts_right = hpx.GlobalMemory.alloc_local_at(1, parts_local_right.shape[0], particle_type, node['right'])
            parts_right[0].set(parts_local_right)
            partition_node(node_right[0], node_right[0], parts_right, parts_local_right.shape[0], n_partition, rsync_lco=reduce_lco)
        else:
            empty = np.zeros(1, dtype=moment_type)
            reduce_lco.set(array=empty)

        hpx.call_cc(save_and_continue_moments, hpx.thread_current_target(), node_gas, reduce_lco, gate=reduce_lco)
    
    return hpx.SUCCESS

@hpx.create_action()
def save_and_continue_moments(node_gas, moments_lco):
    node = node_gas.try_pin()
    node['moments'] = moments_lco.get()
    moments_lco.delete()
    hpx.thread_continue('array', node['moments'])
    return hpx.SUCCESS

@hpx.create_action()
def spawn_computation(current, root, sync, theta): # (int, GlobalMemory, LCO, float)
    current_local = hpx.GlobalAddress(current).try_pin()
    node = hpx.construct_array(current_local, (1,), node_type)[0]
    if(is_leaf(node)):
        parts_addr = hpx.GlobalAddress(node['parts'], node['count']*particle_type.itemsize)
        parts_gas = hpx.GlobalAddressBlock(parts_addr, (node['count'],), particle_type, (particle_type.itemsize,))
        parts = parts_gas.try_pin()
        for i in range(node['count']):
            compute_and_save(hpx.HERE(), root, sync, parts_gas[i], parts[i]['pos'], theta)
        parts_gas.unpin()
    else:
        if node['left'] != hpx.NULL().addr:
            spawn_computation(node['left'], node['left'], root, sync, theta)
        if node['right'] != hpx.NULL().addr:
            spawn_computation(node['right'], node['right'], root, sync, theta)
        
    return hpx.SUCCESS

@hpx.create_action()
def compute_and_save(root, sync, current, pos, theta):
    compdone = hpx.Future(shape=(1,), dtype=np.dtype(float))
    node_compute_potential(root[0], root.addr.addr, pos, theta, rsync_lco=compdone)
    particle_set_approx(current, current, compdone, gate=compdone, rsync_lco=sync)
    return hpx.SUCCESS

@hpx.create_action()
def node_compute_potential(current, pos, theta): # (int, float, float)
    print(current)
    current_gas = hpx.GlobalAddress(current)
    node = hpx.construct_array(current_gas.try_pin(), (1,), node_type)[0]
    dist = pos - node['moments']['xcom']
    size = node['high'] - node['low']
    test = size / dist
    if test < theta:
        retval = node_compute_approx(node, pos)
        hpx.thread_continue('array', retval)
    elif is_leaf(node):
        parts_gas = hpx.GlobalAddress(node['parts'])
        particle_array = hpx.construct_array(parts_gas.try_pin(), (node['count'],), 
                                             particle_type)
        retval = node_compute_direct(particle_array, pos)
        parts_gas.unpin()
        hpx.thread_continue('array', retval)
    else:
        potential_lco = hpx.Reduce(2, (1,), np.dtype(float), float_sum_id, float_sum_op)
        if node['left'] != hpx.NULL().addr:
            node_compute_potential(node['left'], node['left'], pos, theta, rsync_lco=potential_lco)
        else:
            potential_lco.set(array=np.array([0.0], dtype=float), sync='async')

        if node['right'] != hpx.NULL().addr:
            node_compute_potential(node['right'], node['right'], pos, theta, rsync_lco=potential_lco)
        else:
            potential_lco.set(array=np.array([0.0], dtype=float), sync='async')
        hpx.call_cc(potential_reduction_complete, potential_lco.addr, potential_lco, gate=potential_lco)
    
    current_gas.unpin()
    return hpx.SUCCESS

@hpx.create_action()
def particle_set_approx(current, compdone): # (GlobalAddressBlock, LCO)
    current_local = current.try_pin()
    current.unpin()
    return hpx.SUCCESS

@hpx.create_id_action(np.dtype(float))
def float_sum_id(array):
    array[:] = 0

@hpx.create_op_action(np.dtype(float))
def float_sum_op(lhs, rhs):
    lhs[:] = lhs + rhs

@hpx.create_action()
def potential_reduction_complete(potential_lco):
    contval = potential_lco.get()
    potential_lco.delete()
    hpx.thread_continue('array', contval)
    return hpx.SUCCESS

def is_leaf(node):
    return (node['left'] == hpx.NULL().addr) and (node['right'] == hpx.NULL().addr)

def print_usage():
    print("Usage: python3 tree.py <N parts> <Partition Limit> <theta> <domain size>")

def domain_low_bound(which):
    return (domain_size / domain_count) * which

def domain_high_bound(which):
    return (domain_size / domain_count) * (which + 1)

def map_bound_to_locality(low, high):
    domain_span = domain_size / domain_count
    low_idx = int(low // domain_span)
    high_idx = int(high // domain_span)

    if low_idx == high_idx:
        return low_idx

    if high_idx - low_idx == 1:
        delta_low = domain_low_bound(high_idx) - low
        delta_high = high - domain_high_bound(low_idx)
        if delta_high > delta_low:
            return high_idx
        else:
            return low_idx

    return low_idx

def create_node(low, high):
    # this function returns a GlobalMemory object for the created node
    where = map_bound_to_locality(low, high)
    rtv = hpx.GlobalMemory.alloc_local_at(1, 1, node_type, hpx.THERE(where))
    vals = np.array([(hpx.NULL().addr, hpx.NULL().addr, low, high, np.array([(0.0, 0.0, 0.0)], dtype=moment_type),
                      hpx.NULL().addr, 0)], dtype=node_type)
    rtv[0].set(vals)
    return rtv

def generate_parts(n_parts, domain_length, where):
    parts_gas = hpx.GlobalMemory.alloc_local_at(1, n_parts, particle_type, where)
    parts = parts_gas[0].try_pin()
    parts['pos'] = np.random.rand(n_parts) * domain_length
    parts['mass'] = np.random.rand(n_parts) * 0.9 + 0.1
    parts_gas[0].unpin()
    return parts_gas

def node_compute_approx(node, pos):
    return (-node['moments']['mtot']/abs(pos - node['moments']['xcom'])
            - node['moments']['Q00']/(2 * abs(pos - node['moments']['xcom'])**3))

def node_compute_direct(particles, pos):
    a = -particles['mass']/np.abs(particles['pos'] - pos)
    return np.sum(a)

if __name__ == '__main__':
    try:
        hpx.init(sys.argv)
    except HPXError:
        # TODO
        pass

    if len(sys.argv) != 5:
        print_usage()
        hpx.finalize()
        sys.exit(0)

    # convert arguments to numerical values
    n_parts, n_partition, theta_c, domain_size = sys.argv[1:]
    n_parts = int(n_parts)
    n_partition = int(n_partition)
    theta_c = float(theta_c)
    domain_size = float(domain_size)

    hpx.run(main, n_parts, n_partition, theta_c, domain_size)

    hpx.finalize()
