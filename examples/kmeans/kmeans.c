#include <hpx/hpx.h>
#include <stdio.h>
#include <math.h>
#include <time.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#define DATA_SIZE 10000000
#define DATA_RANGE 100000.0
#define DATA_PER_NODE 1000000
#define K 100 // in current implementation, K <= DATA_PER_NODE
#define MAX_ITERATION 10
#define DEBUG_FLAG 0

static HPX_ACTION_DECL(main_action);
static HPX_ACTION_DECL(generate_data_action);
static HPX_ACTION_DECL(kmeans_action);
static HPX_ACTION_DECL(broadcast_centers_action);
static HPX_ACTION_DECL(set_zero_action);
static HPX_ACTION_DECL(sum_position_action);
static HPX_ACTION_DECL(sum_count_action);
static float centers[K][2];

static int main_handler(void) {
    const int num_node = ceil(DATA_SIZE/DATA_PER_NODE);
    const int num_data_per_node = DATA_PER_NODE; 
    const int num_data_last_node = DATA_SIZE - (num_node - 1)*DATA_PER_NODE;

    hpx_addr_t data = hpx_gas_alloc_cyclic(num_node, DATA_PER_NODE*sizeof(float)*2, 0); 
    hpx_addr_t generate_data_complete = hpx_lco_and_new(num_node);
    for(int i = 0; i < num_node; i++) {
        hpx_addr_t target = hpx_addr_add(data, i*DATA_PER_NODE*sizeof(float)*2, DATA_PER_NODE*sizeof(float)*2);
        if (i < num_node - 1) {
            hpx_call(target, generate_data_action, generate_data_complete, &num_data_per_node);
        } else {
            hpx_call(target, generate_data_action, generate_data_complete, &num_data_last_node);
        }
    }
    hpx_lco_wait(generate_data_complete);
    hpx_lco_delete_sync(generate_data_complete);
    
    hpx_gas_memget_sync(centers, data, sizeof(centers));

    int num_changes = 1;
    int num_iteration = 0;
    while(num_iteration < MAX_ITERATION && num_changes > 0) {
        fprintf(stderr, "start iteration %d\n", num_iteration);
        hpx_bcast_rsync(broadcast_centers_action, centers, sizeof(centers)); 
        hpx_addr_t position_reduce = hpx_lco_reduce_new(num_node, K*2*sizeof(float), set_zero_action, sum_position_action);
        hpx_addr_t count_reduce = hpx_lco_reduce_new(num_node, K*sizeof(int), set_zero_action, sum_count_action);
        for(int i = 0; i < num_node; i++) {
            hpx_addr_t target = hpx_addr_add(data, i*num_data_per_node*sizeof(float)*2, num_data_per_node*sizeof(float)*2);
            if (i < num_node - 1) {
                hpx_call(target, kmeans_action, HPX_NULL, &num_data_per_node, &position_reduce, &count_reduce);
            } else {
                hpx_call(target, kmeans_action, HPX_NULL, &num_data_last_node, &position_reduce, &count_reduce);
            }
        }
        float positions[K][2];
        int count[K];
        hpx_lco_get(position_reduce, K*2*sizeof(float), positions);
        hpx_lco_get(count_reduce, K*sizeof(int), count);
        for(int i = 0; i < K; i++) {
            centers[i][0] = positions[i][0] / count[i];
            centers[i][1] = positions[i][1] / count[i];
        }
        hpx_lco_delete_sync(position_reduce);
        hpx_lco_delete_sync(count_reduce);
        num_iteration ++;
    }

    hpx_exit(HPX_SUCCESS);
}
static HPX_ACTION(HPX_DEFAULT, 0, main_action, main_handler);


static int generate_data_handler(void* datachunk, int size) {
    float* data = (float*) datachunk;
    srand(time(NULL));
    for(int i = 0; i < size; i++) {
        data[2*i] = ((double)rand()/RAND_MAX)*DATA_RANGE;
        data[2*i+1] = ((double)rand()/RAND_MAX)*DATA_RANGE;
    }
    return HPX_SUCCESS;
}
static HPX_ACTION(HPX_DEFAULT, HPX_PINNED,
        generate_data_action, generate_data_handler,
        HPX_POINTER, HPX_INT);


static int broadcast_centers_handler(void* buffer, size_t bytes) {
    memcpy(centers, buffer, bytes);
    return HPX_SUCCESS;
}
static HPX_ACTION(HPX_DEFAULT, HPX_MARSHALLED,
        broadcast_centers_action, broadcast_centers_handler,
        HPX_POINTER, HPX_SIZE_T);


static int set_zero_handler(void* data, size_t bytes) {
    memset(data, 0, bytes);
}
static HPX_ACTION(HPX_FUNCTION, 0,
        set_zero_action, set_zero_handler,
        HPX_POINTER, HPX_SIZE_T);


static int sum_count_handler(int* lhs, const int* rhs, size_t bytes) {
    for(int i = 0; i < K; i++) {
        lhs[i] += rhs[i];
    }
}
static HPX_ACTION(HPX_FUNCTION, 0,
        sum_count_action, sum_count_handler,
        HPX_POINTER, HPX_SIZE_T);


static int sum_position_handler(float lhs[K][2], const float rhs[K][2], size_t bytes) {
    for(int i = 0; i < K; i++) {
        lhs[i][0] += rhs[i][0];
        lhs[i][1] += rhs[i][1];
    }
}
static HPX_ACTION(HPX_FUNCTION, 0,
        sum_position_action, sum_position_handler,
        HPX_POINTER, HPX_POINTER, HPX_SIZE_T);

float calc_distance_sq(const float* pt0, const float* pt1) {
    return (pt0[0]-pt1[0])*(pt0[0]-pt1[0]) + (pt0[1]-pt1[1])*(pt0[1]-pt1[1]);
}

static int kmeans_handler(void* datachunk, int size, hpx_addr_t position_reduce, hpx_addr_t count_reduce) {
    const float* data = (const float*)datachunk; 
    int count[K];
    float sum[K][2];
    int* label = (int *)malloc(size*sizeof(int));
    int num_change = 1;

    memset(count, 0, sizeof(count));
    memset(sum, 0, sizeof(sum));
    for(int i = 0; i < size; i++) {
        label[i] = -1;
    }
    
    for(int i = 0; i < size; i++) {
        int mincluster = 0;
        float mindist = calc_distance_sq(&data[2*i], (float *)centers);
        for(int j = 1; j < K; j++) {
            float currdist = calc_distance_sq(&data[2*i], (float *)&centers[2*j]);
            if(currdist < mindist) {
                mincluster = j;
                mindist = currdist;
            }
        }
        count[mincluster] += 1;
        sum[mincluster][0] += data[2*i];
        sum[mincluster][1] += data[2*i+1];
        label[i] = mincluster;
    }

    hpx_lco_set_lsync(position_reduce, K*2*sizeof(float), sum, HPX_NULL);
    hpx_lco_set_lsync(count_reduce, K*sizeof(int), count, HPX_NULL);

    free(label);
    return HPX_SUCCESS;
}
static HPX_ACTION(HPX_DEFAULT, HPX_PINNED,
        kmeans_action, kmeans_handler,
        HPX_POINTER, HPX_INT, HPX_ADDR, HPX_ADDR);


int main(int argc, char* argv[argc]) {
    int debug_wait = DEBUG_FLAG;
    if(debug_wait) {
        char hostname[256];
        gethostname(hostname, sizeof(hostname));
        fprintf(stderr, "PID %d on %s ready for attach\n", getpid(), hostname);
    }
    while(debug_wait)
        sleep(5);
    if(hpx_init(&argc, &argv) != 0)
        return -1;
    int success = hpx_run(&main_action);
    hpx_finalize();
    return success;
}
    
