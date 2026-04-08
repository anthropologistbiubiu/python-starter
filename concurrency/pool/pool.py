from multiprocessing import Pool, cpu_count
import os
import time


def long_time_task(i):
    print('子进程: {} - 任务{}'.format(os.getpid(), i))
    time.sleep(2)
    print("结果: {}".format(8 ** 20))


if __name__=='__main__':
    print("CPU内核数:{}".format(cpu_count()))
    print('当前母进程: {}'.format(os.getpid()))
    start = time.time()
    p = Pool(4)
    for i in range(5):
        p.apply_async(long_time_task, args=(i,))
    #apply_async
    # 函数原型：apply_async(func[, args=()[, kwds={}[, callback=None]]])
    # 其作用是向进程池提交需要执行的函数及参数， 各个进程采用非阻塞（异步）的调用方式，
    # 即每个子进程只管运行自己的，不管其它进程是否已经完成。这是默认方式。
    print('等待所有子进程完成。')
    p.close()
    p.join()
    end = time.time()
    print("总共用时{}秒".format((end - start)))
