#include <stdlib.h>
#include <time.h>
#include <stdio.h>
#include <mpi.h>
#include <hpx/hpx.h> 

#define DIM 1024

static hpx_action_t _main;
static hpx_action_t _init;
static hpx_action_t sum_init;
static hpx_action_t sum_op;
static hpx_action_t prod;

int main(int argc, char* argv[]) {

    int _main_action(double* x, double* y);
    hpx_register_action(HPX_DEFAULT, 0, "_main", &_main, 3, _main_action, HPX_POINTER, HPX_POINTER); 
    int _init_action(hpx_addr_t target, double x, double y);
    hpx_register_action(HPX_DEFAULT, 0, "_init", &_init, 4, 
                        _init_action, HPX_ADDR, HPX_DOUBLE, HPX_DOUBLE); 
    void sum_init_action(double *input, const size_t bytes);
    hpx_register_action(HPX_FUNCTION, 0, "sum_init", &sum_init, 1, sum_init_action);
    void sum_op_action(double* lhs, const double* rhs, size_t UNUSED);
    hpx_register_action(HPX_FUNCTION, 0, "sum_op", &sum_op, 1, sum_op_action);
    int prod_action(hpx_addr_t prod_lco);
    hpx_register_action(HPX_DEFAULT, 0, "prod", &prod, 2, prod_action, HPX_ADDR);

    /* Initialize runtime */
    if(hpx_init(&argc, &argv) != 0)
        return -1;   
    int flag;
    MPI_Initialized(&flag);
    if(flag == 0) {
        MPI_Init(&argc, &argv); 
        printf("call MPI_Init\n");
    }
    
    int total_rank, curr_rank;
    MPI_Comm_rank(MPI_COMM_WORLD, &curr_rank);
    MPI_Comm_size(MPI_COMM_WORLD, &total_rank);

    double* global_x = (double *)malloc(sizeof(double)*DIM);
    double* global_y = (double *)malloc(sizeof(double)*DIM);
    double* local_x = (double *)malloc(sizeof(double)*DIM/total_rank);
    double* local_y = (double *)malloc(sizeof(double)*DIM/total_rank);

    /* Generate data */
    if(curr_rank == 0) {
        srand((unsigned int)time(NULL));
        for(int i = 0; i < DIM; i++)
            global_x[i] = (double)rand()/RAND_MAX;
        
        srand((unsigned int)time(NULL));
        for(int i = 0; i < DIM; i++)
            global_y[i] = (double)rand()/RAND_MAX;

        double result = 0;
        for(int i = 0; i < DIM; i++)
            result += global_x[i]*global_y[i];
        
        printf("Result using one rank: %lf\n", result);
    }

    /* distributed using MPI */

    MPI_Scatter(global_x, DIM/total_rank, MPI_DOUBLE, 
                local_x, DIM/total_rank, MPI_DOUBLE, 
                0, MPI_COMM_WORLD);
    MPI_Scatter(global_y, DIM/total_rank, MPI_DOUBLE, 
                local_y, DIM/total_rank, MPI_DOUBLE, 
                0, MPI_COMM_WORLD);
    
    double local_result = 0;
    for(int i = 0; i < DIM/total_rank; i++) 
        local_result += local_x[i]*local_y[i];

    double global_result;
    MPI_Reduce(&local_result, &global_result, 1, MPI_DOUBLE, MPI_SUM, 0, MPI_COMM_WORLD);
    if(curr_rank == 0)
        printf("Result distributed using MPI: %lf\n", global_result); 

    /* distributed using HPX */
    hpx_run(&_main, &global_x, &global_y);

    /* Cleanup runtime */
    hpx_finalize();
    MPI_Finalized(&flag);
    if(flag == 0) {
        MPI_Finalize();
        printf("call MPI_Finalize!\n");
    }

    return 0;
}

static hpx_addr_t custom_addr_add(hpx_addr_t addr, int block_no) {
    return hpx_addr_add(addr, block_no*sizeof(double)*2, sizeof(double)*2);
}

int _main_action(double* x, double* y) {
    hpx_addr_t hpx_global_xy = hpx_gas_alloc_cyclic(DIM, sizeof(double)*2, 0);
    hpx_addr_t copy_data_lco = hpx_lco_and_new(DIM);

    for(int i = 0; i < DIM; i++) {
        hpx_addr_t current = custom_addr_add(hpx_global_xy, i);
        hpx_call(current, _init, copy_data_lco, &current, &x[i], &y[i]);
    }
    
    hpx_addr_t sum_lco = hpx_lco_reduce_new(DIM, sizeof(double), sum_init, sum_op);
    for(int i = 0; i < DIM; i++) {
        hpx_addr_t current = custom_addr_add(hpx_global_xy, i);
        hpx_call(current, prod, HPX_NULL, &sum_lco);
    }

    double result;
    hpx_lco_get(sum_lco, sizeof(double), &result);
    printf("Result distributed using HPX: %lf\n", result);

    hpx_exit(HPX_SUCCESS);
}

int _init_action(hpx_addr_t target, double x, double y) {
    double* local_target;
    if(hpx_gas_try_pin(target, (void **)&local_target)) {
        local_target[0] = x;
        local_target[1] = y;
        hpx_gas_unpin(target);
        return HPX_SUCCESS;
    } else {
        return HPX_ERROR;
    }
}

void sum_init_action(double *input, const size_t bytes) {
    *input = 0;
}

void sum_op_action(double* lhs, const double* rhs, size_t UNUSED) {
    *lhs += *rhs;
}

int prod_action(hpx_addr_t prod_lco) {
    hpx_addr_t target = hpx_thread_current_target();
    double* local_target;
    if(hpx_gas_try_pin(target, (void **)&local_target)) {
        double prod = local_target[0] * local_target[1];
        hpx_lco_set_rsync(prod_lco, sizeof(double), &prod);  
        hpx_gas_unpin(target);
        return HPX_SUCCESS;
    } else {
        return HPX_ERROR;
    }
}

