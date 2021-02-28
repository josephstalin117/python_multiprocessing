import torch.multiprocessing as mp
import atexit
import bisect
from network import Net
import torch

class Predictor:
    class _StopToken:
        pass

    class _TaskWorker(mp.Process):
        def __init__(self, input_queue, output_queue, device):
            super(Predictor._TaskWorker, self).__init__()
            self.input_queue = input_queue    # the queue to get task
            self.output_queue = output_queue  # the queue to put result
            self.device = device
            
        def run(self):
            # initialization
            model = Net()
            model.to(self.device)
            
            # loop to get and do the tasks
            while True:
                task = self.input_queue.get()  # pick a task from the queue
                if isinstance(task, Predictor._StopToken):  # `_StopToken` is a signal to stop this worker
                    break
                
                # decode task, it can be anything you defined.
                task_id, x = task  
                
                with torch.no_grad():
                    # do the task
                    x = x.to(self.device)
                    output = model(x)
                    
                    # put the result into the queue
                    output = output.cpu()  # copy to cpu before send to another process
                self.output_queue.put((task_id, output))  

    class _ReceiveWorker(mp.Process):
        def __init__(self, receive_func, input_queue, output_queue):
            super(Predictor._ReceiveWorker, self).__init__()
            self.receive_func = receive_func  # the function to get the result from '_TaskWorker'
            self.input_queue = input_queue  
            self.output_queue = output_queue

        def run(self):
            while True:
                task = self.input_queue.get()
                if isinstance(task, Predictor._StopToken):
                    break

                # decode task, get the number of tasks the worker will collect
                length = task

                # collect and postprocess the result from `_TaskWorker`
                data_list = []
                for _ in range(length):
                    # collect data
                    data = self.receive_func()
                    
                    # postprocess data. 
                    data_list.append(data)

                # put the final result into the queue
                data = torch.cat(data_list, dim=0)
                task = self.output_queue.put(data)

    def __init__(self, device_list):
        self.num_proc = len(device_list)  # number of process

        self.put_id = 0
        self.get_id = 0
        self.id_buffer = []
        self.data_buffer = []

        self.task_worker_input_queue = mp.Queue(maxsize=self.num_proc * 3)
        self.task_worker_output_queue = mp.Queue(maxsize=self.num_proc * 3)
        self.receive_worker_input_queue = mp.Queue(maxsize=1)
        self.receive_worker_output_queue = mp.Queue(maxsize=1)

        # create workers
        self.task_workers = []
        for device in device_list:
            self.task_workers.append(Predictor._TaskWorker(self.task_worker_input_queue, self.task_worker_output_queue, device))

        self.receive_worker = Predictor._ReceiveWorker(self.get_from_task_worker, self.receive_worker_input_queue, self.receive_worker_output_queue)

        # start workers
        for worker in self.task_workers:
            worker.start()
        self.receive_worker.start()

        atexit.register(self.shutdown)

    def __del__(self):
        self.shutdown()

    def shutdown(self):
        for _ in range(self.num_proc):
            self.task_worker_input_queue.put(Predictor._StopToken())
        self.receive_worker_input_queue.put(Predictor._StopToken())

    def put_into_task_worker(self, data):
        task_id = self.put_id
        self.put_id += 1
        self.task_worker_input_queue.put((task_id, data))

    def get_from_task_worker(self):
        if len(self.id_buffer) and self.id_buffer[0] == self.get_id:
            data = self.data_buffer[0]
            del self.id_buffer[0], self.data_buffer[0]
            self.get_id += 1
            return data
        
        while True:
            task_id, data = self.task_worker_output_queue.get()
            if task_id == self.get_id:
                self.get_id += 1
                return data
            insert_position = bisect.bisect(self.id_buffer, task_id)
            self.id_buffer.insert(insert_position, task_id)
            self.data_buffer.insert(insert_position, data)
    

    def put_into_receive_worker(self, data):
        self.receive_worker_input_queue.put(data)
    
    def get_from_receive_worker(self):
        return self.receive_worker_output_queue.get()

    def __call__(self, data_list):
        """
        Args:
            data_list (list[float]): input data list
        
        Returns:
            torch.FloatTensor:
        """
        # inform the receive worker the number of data to receive
        self.put_into_receive_worker(len(data_list))

        # put data to task worker
        for data in data_list:
            data = torch.Tensor([[data]])  # preprocess data
            self.put_into_task_worker(data)

        # get result from receive worker
        result = self.get_from_receive_worker()
        return result


if __name__ == '__main__':
    device_list = ["cuda:0", "cuda:1", "cuda:2", "cuda:3"]
    predictor = Predictor(device_list)
    
    data_list = list(range(100))
    result = predictor(data_list)

    print(result.shape)

    del predictor  # delete the object to remove process manually, otherwise these process will be auto closed when the program exits.