package node

// Node represents a worker host in the cluster.
type Node struct {
	Name            string
	Ip              string
	Api             string
	Cores           int
	Memory          int64
	MemoryAllocated int64
	Disk            int64
	DiskAllocated   int64
	Role            string
	TaskCount       int
}
