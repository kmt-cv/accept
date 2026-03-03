import subprocess
import time
import os
import sys
import resource

from core import globalLogger, globalSettings

PASSTHROUGH_ARGS = {
    "log_level" : "INFO",
    "cache_dir" : "cache",
    "config" : ""
}
WORKER_START_CMDLINE = f"{sys.executable} {sys.argv[0]} process " + " ".join([ f"--{arg} {getattr(globalSettings.args,arg)}" for arg, default in PASSTHROUGH_ARGS.items() if getattr(globalSettings.args,arg) != default ])

def limitVirtualMemory():
    resource.setrlimit(resource.RLIMIT_AS, (globalSettings.args.flush_thread_max_memory, resource.RLIM_INFINITY))

if globalSettings.args.debug:
    WORKER_START_CMDLINE = WORKER_START_CMDLINE.replace(sys.executable, f"{sys.executable} -Xfrozen_modules=off")

class Process:
    pid:int
    startTime:float
    cache:str
    process:subprocess.Popen

    def __init__(self, cache:str) -> None:
        self.cache = cache
        self.process = subprocess.Popen(f"{WORKER_START_CMDLINE} --cache {cache}".split(" "),start_new_session=False,stdout=subprocess.PIPE, stderr=subprocess.STDOUT,preexec_fn=limitVirtualMemory)
        os.set_blocking(self.process.stdout.fileno(), False)
        self.startTime = time.time()
        self.pid = self.process.pid

class TaskPool:
    running:list[Process]
    waiting:set

    def __init__(self) -> None:
        self.running = []
        self.waiting = set()

taskPool = TaskPool()

def remainingCapacity(includeWaiting=True) -> int:
    if includeWaiting:
        return globalSettings.args.flush_threads - ( len(taskPool.running) + len(taskPool.waiting) )
    else:
        return globalSettings.args.flush_threads - len(taskPool.running)

def register(cache:str):
    if cache not in taskPool.waiting and not any([ x.cache == cache for x in taskPool.running ]):
        taskPool.waiting.add(cache)
    globalLogger.logger.log(6,"Task Registered",{ "cache" : cache },extra={ "source" : "queue", "type" : "register" })

def kill(cache: str):
    for task in taskPool.waiting:
        if task == cache:
            taskPool.waiting.remove(task)
            return True
    for process in taskPool.running:
        if process.cache == cache:
            process.process.terminate()
            taskPool.running.remove(process)
            return True
    return False

def process(self):
    startTime = time.perf_counter()
    cacheFile = os.path.join(globalSettings.args.cache_dir, globalSettings.args.cache)
    processingMarker = f"{cacheFile}.processing"

    # Check if the cache file exists
    if not os.path.exists(cacheFile): 
        self.logger.log(50, f"Cache file does not exist", {
            "name": self.name, 
            "id": self.id, 
            "cache": globalSettings.args.cache
        }, extra={"source": "cache", "type": "exception"})
        return

    # Check for an existing processing marker
    if os.path.exists(processingMarker):
        self.logger.log(40, f"Processing marker found, skipping cache file", {
            "name": self.name, 
            "id": self.id, 
            "cache": globalSettings.args.cache
        }, extra={"source": "cache", "type": "warning"})
        return

    # Create the processing marker
    open(processingMarker, 'w').close()

    try:
        cacheSize = os.path.getsize(cacheFile)
        with open(cacheFile) as f:
            for event in f:
                eventStartTime = time.perf_counter_ns()
                try:
                    for next in self.next if self.next else []:
                        next.processHandler(event.strip(), stack=[self.id])
                except Exception as e:
                    if self.nextError and self.nextError in objectCache.objectCache:
                        globalLogger.logger.log(6, f"Event Exception Running Next Error", {
                            "name": self.name, 
                            "id": self.id
                        }, extra={"source": "input", "type": "next_error"}, exc_info=True)
                        objectCache.objectCache[self.nextError].processHandler(event.strip(), stack=[self.id])
                    else:
                        raise
                self.updateProcessStats(eventStartTime)
        for item in postRegister.items:
            item()
        os.remove(cacheFile)
        self.logger.log(7, f"Cache file processed", {
            "name": self.name, 
            "id": self.id, 
            "cache": globalSettings.args.cache, 
            "took": time.perf_counter() - startTime, 
            "size": cacheSize
        }, extra={"source": "cache", "type": "stats"})
    except Exception as e:
        self.logger.log(40, f"Error processing cache file", {
            "name": self.name, 
            "id": self.id, 
            "cache": globalSettings.args.cache
        }, extra={"source": "cache", "type": "exception"}, exc_info=True)
    finally:
        # Remove the processing marker
        if os.path.exists(processingMarker):
            os.remove(processingMarker)