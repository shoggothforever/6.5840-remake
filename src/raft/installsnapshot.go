package raft

type InstallSnapshotArgs struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Offset            int
	Data              []byte
	Done              bool
}
type InstallSnapshotRes struct {
	Term int
}

func (rf *Raft) CallInstallSnapshot(server int, args *InstallSnapshotArgs, res *InstallSnapshotRes) {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, res)
	if !ok {
		return
	}

}
func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, res *InstallSnapshotRes) bool {
	return true
}
