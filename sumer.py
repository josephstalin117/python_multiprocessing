import multiprocessing as mp
import atexit

class Sumer:
    class _StopToken:
        pass

    class _TaskWorker(mp.Process):
        def __init__(self, input_queue, output_queue):
            super(Sumer._TaskWorker, self).__init__()
            self.input_queue = input_queue    # the queue to get task
            self.output_queue = output_queue  # the queue to put result
            
        def run(self):
            # initialization
            # anything
            
            # loop to get and do the tasks
            while True:
                task = self.input_queue.get()  # pick a task from the queue
                if isinstance(task, Sumer._StopToken):  # `_StopToken` is a signal to stop this worker
                    break
                
                # decode task, it can be anything you defined.
                num = task  
                
                # do the task. it's to get the suquare of the `num` here.
                result = num * num
                
                # put the result into the queue
                self.output_queue.put(result)  

    class _ReceiveWorker(mp.Process):
        def __init__(self, receive_func, input_queue, output_queue):
            super(Sumer._ReceiveWorker, self).__init__()
            self.receive_func = receive_func  # the function to get the result from '_TaskWorker'
            self.input_queue = input_queue  
            self.output_queue = output_queue

        def run(self):
            while True:
                task = self.input_queue.get()
                if isinstance(task, Sumer._StopToken):
                    break

                # decode task, get the number of tasks the worker will collect
                length = task

                # collect and postprocess the result from `_TaskWorker`
                sum = 0
                for _ in range(length):
                    # collect data
                    data = self.receive_func()
                    
                    # postprocess data. 
                    sum += data

                # put the final result into the queue
                task = self.output_queue.put(sum)

    def __init__(self, num_proc):
        self.num_proc = num_proc  # number of process

        self.task_worker_input_queue = mp.Queue(maxsize=self.num_proc * 3)
        self.task_worker_output_queue = mp.Queue(maxsize=self.num_proc * 3)
        self.receive_worker_input_queue = mp.Queue(maxsize=1)
        self.receive_worker_output_queue = mp.Queue(maxsize=1)

        # create workers
        self.task_workers = []
        for _ in range(self.num_proc):
            self.task_workers.append(Sumer._TaskWorker(self.task_worker_input_queue, self.task_worker_output_queue))

        self.receive_worker = Sumer._ReceiveWorker(self.get_from_task_worker, self.receive_worker_input_queue, self.receive_worker_output_queue)

        # start workers
        for worker in self.task_workers:
            worker.start()
        self.receive_worker.start()

        atexit.register(self.shutdown)

    def __del__(self):
        self.shutdown()

    def shutdown(self):
        for _ in range(self.num_proc):
            self.put_into_task_worker(Sumer._StopToken())
        self.put_into_receive_worker(Sumer._StopToken())

    def put_into_task_worker(self, data):
        self.task_worker_input_queue.put(data)

    def get_from_task_worker(self):
        return self.task_worker_output_queue.get()

    def put_into_receive_worker(self, data):
        self.receive_worker_input_queue.put(data)
    
    def get_from_receive_worker(self):
        return self.receive_worker_output_queue.get()

    def __call__(self, data_list):
        """
        Args:
            data_list (list[float]):
        
        Returns:
            float: sum of squares
        """
        # inform the receive worker the number of data to receive
        self.put_into_receive_worker(len(data_list))

        # put data to task worker
        for data in data_list:
            self.put_into_task_worker(data)

        # get result from receive worker
        result = self.get_from_receive_worker()
        return result


if __name__ == '__main__':
    num_proc = 3
    sumer = Sumer(num_proc)
    
    data_list = list(range(1000))
    result = sumer(data_list)

    print(result)

    del sumer  # delete the object to remove process manually, otherwise these process will be auto closed when the program exits.