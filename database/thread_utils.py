import threading

def run_threaded(job_func, userdata):
    job_thread = threading.Thread(target=job_func, args=(userdata,))
    job_thread.start()
