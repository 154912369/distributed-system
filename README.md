# distributed-system
基于mit6.824课程的代码
#课程原地址是http://nil.csail.mit.edu/6.824/2018/general.html

2020年3月12日
#主要完成test2A部分，但是没有加锁，可以pass。测试方法是在src/raft文件中运行 go test -run 2A.
#另注：src/main中完成了mapreduce中map部分，因为rpc中存在传输参数的位置问题而未解决reduce部分,运行方式可见https://pdos.csail.mit.edu/6.824/labs/lab-mr.html。
