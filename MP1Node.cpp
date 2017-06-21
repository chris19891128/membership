/**********************************
 * FILE NAME: MP1Node.cpp
 *
 * DESCRIPTION: Membership protocol run by this Node.
 * 				Definition of MP1Node class functions.
 **********************************/

#include "MP1Node.h"

/*
 * Note: You can change/add any functions in MP1Node.{h,cpp}
 */

/**
 * Overloaded Constructor of the MP1Node class
 * You can add new members to the class if you think it
 * is necessary for your logic to work
 */
MP1Node::MP1Node(Member *member, Params *params, EmulNet *emul, Log *log, Address *address) {
	for( int i = 0; i < 6; i++ ) {
		NULLADDR[i] = 0;
	}
	this->memberNode = member;
	this->emulNet = emul;
	this->log = log;
	this->par = params;
	this->memberNode->addr = *address;
}

/**
 * Destructor of the MP1Node class
 */
MP1Node::~MP1Node() {}

/**
 * FUNCTION NAME: recvLoop
 *
 * DESCRIPTION: This function receives message from the network and pushes into the queue
 * 				This function is called by a node to receive messages currently waiting for it
 */
int MP1Node::recvLoop() {
    if ( memberNode->bFailed ) {
    	return false;
    }
    else {
    	return emulNet->ENrecv(&(memberNode->addr), enqueueWrapper, NULL, 1, &(memberNode->mp1q));
    }
}

/**
 * FUNCTION NAME: enqueueWrapper
 *
 * DESCRIPTION: Enqueue the message from Emulnet into the queue
 */
int MP1Node::enqueueWrapper(void *env, char *buff, int size) {
	Queue q;
	return q.enqueue((queue<q_elt> *)env, (void *)buff, size);
}

/**
 * FUNCTION NAME: nodeStart
 *
 * DESCRIPTION: This function bootstraps the node
 * 				All initializations routines for a member.
 * 				Called by the application layer.
 */
void MP1Node::nodeStart(char *servaddrstr, short servport) {
    Address joinaddr;
    joinaddr = getJoinAddress();

    // Self booting routines
    if( initThisNode(&joinaddr) == -1 ) {
#ifdef DEBUGLOG
        log->LOG(&memberNode->addr, "init_thisnode failed. Exit.");
#endif
        exit(1);
    }

    if( !introduceSelfToGroup(&joinaddr) ) {
        finishUpThisNode();
#ifdef DEBUGLOG
        log->LOG(&memberNode->addr, "Unable to join self to group. Exiting.");
#endif
        exit(1);
    }

    return;
}

/**
 * FUNCTION NAME: initThisNode
 *
 * DESCRIPTION: Find out who I am and start up
 */
int MP1Node::initThisNode(Address *joinaddr) {
	/*
	 * This function is partially implemented and may require changes
	 */
	int id = *(int*)(&memberNode->addr.addr);
	int port = *(short*)(&memberNode->addr.addr[4]);

	memberNode->bFailed = false;
	memberNode->inited = true;
	memberNode->inGroup = false;
    // node is up!
	memberNode->nnb = 0;
	memberNode->heartbeat = 0;
	memberNode->pingCounter = TFAIL;
	memberNode->timeOutCounter = -1;
    initMemberListTable(memberNode);

    return 0;
}

/**
 * FUNCTION NAME: introduceSelfToGroup
 *
 * DESCRIPTION: Join the distributed system
 */
int MP1Node::introduceSelfToGroup(Address *joinaddr) {
	MessageHdr *msg;
#ifdef DEBUGLOG
    static char s[1024];
#endif

    if ( 0 == memcmp((char *)&(memberNode->addr.addr), (char *)&(joinaddr->addr), sizeof(memberNode->addr.addr))) {
        // I am the group booter (first process to join the group). Boot up the group
#ifdef DEBUGLOG
        log->LOG(&memberNode->addr, "Starting up group...");
#endif
        memberNode->inGroup = true;

        updateMemberList(&memberNode->addr, memberNode->heartbeat);

    }
    else {
        size_t msgsize = sizeof(MessageHdr) + sizeof(joinaddr->addr) + sizeof(long) + 1;
        msg = (MessageHdr *) malloc(msgsize * sizeof(char));

        // create JOINREQ message: format of data is {struct Address myaddr}
        msg->msgType = JOINREQ;
        memcpy((char *)(msg+1), &memberNode->addr.addr, sizeof(memberNode->addr.addr));
        memcpy((char *)(msg+1) + 1 + sizeof(memberNode->addr.addr), &memberNode->heartbeat, sizeof(long));

#ifdef DEBUGLOG
        sprintf(s, "Trying to join...");
        log->LOG(&memberNode->addr, s);
#endif

        // send JOINREQ message to introducer member
        emulNet->ENsend(&memberNode->addr, joinaddr, (char *)msg, msgsize);

        free(msg);
    }

    return 1;

}

/**
 * FUNCTION NAME: finishUpThisNode
 *
 * DESCRIPTION: Wind up this node and clean up state
 */
int MP1Node::finishUpThisNode(){
   /*
    * Your code goes here
    */
}

/**
 * FUNCTION NAME: nodeLoop
 *
 * DESCRIPTION: Executed periodically at each member
 * 				Check your messages in queue and perform membership protocol duties
 */
void MP1Node::nodeLoop() {
    if (memberNode->bFailed) {
    	return;
    }

    // Debug
    cout << "Dumping member list for " << memberNode->addr.getAddress() << endl;
    vector<MemberListEntry>::iterator it = memberNode->memberList.begin();
    for (; it != memberNode->memberList.end(); ++it) {
        cout << it->id << " " << it->port << " " << it->heartbeat << " " << it->timestamp << endl;
    }
    cout << "Done" << endl;

    memberNode->heartbeat++;

    // Check my messages
    checkMessages();

    // Wait until you're in the group...
    if( !memberNode->inGroup ) {
    	return;
    }

    updateMemberList(&memberNode->addr, memberNode->heartbeat);

    // ...then jump in and share your responsibilites!
    nodeLoopOps();

    return;
}

/**
 * FUNCTION NAME: checkMessages
 *
 * DESCRIPTION: Check messages in the queue and call the respective message handler
 */
void MP1Node::checkMessages() {
    void *ptr;
    int size;

    // Pop waiting messages from memberNode's mp1q
    while ( !memberNode->mp1q.empty() ) {
    	ptr = memberNode->mp1q.front().elt;
    	size = memberNode->mp1q.front().size;
    	memberNode->mp1q.pop();
    	recvCallBack((void *)memberNode, (char *)ptr, size);
    }
    return;
}

/**
 * FUNCTION NAME: recvCallBack
 *
 * DESCRIPTION: Message handler for different message types
 */
bool MP1Node::recvCallBack(void *env, char *data, int size ) {
	/*
	 * Your code goes here
	 */
     // cout << "size of MessageHdr " << sizeof(MessageHdr) << endl;
     // cout << "size of long " << sizeof(long) << endl;
     // for(int i=0;i<size;i++) {
     //    cout << (int)(*(data + i)) << " ";
     // }
     // cout << endl;
    //    size of MessageHdr 4
    // size of long 8
    // 0 0 0 0 2 0 0 0 0 0 0 0 0 0 0 0 0 0 0 

    MessageHdr *msg;
    msg = (MessageHdr *)data;
    int addrSize = sizeof(memberNode->addr.addr);
    
    if (msg->msgType == JOINREQ) {
        // process request message
        char *p = (char *) (msg + 1);
        Address remoteAddr;
        memcpy(&(remoteAddr.addr), p, addrSize);
        
        long hb = *(long *) (p + addrSize);

        updateMemberList(&remoteAddr, hb);

        // How to reply to remoteAddr, what is in the payload
        size_t replySize;
        char *reply = makeMemberListMsg(JOINREP, &replySize);

        cout << memberNode->addr.getAddress() << " Sending JOINREP to " << remoteAddr.getAddress() << endl;
        
        for(int i=0;i<replySize;i++) {
            cout << (int)*(reply+i) << " ";
        }
        cout << "Message size is " << replySize << endl;

        emulNet->ENsend(&memberNode->addr, &remoteAddr, reply, replySize);

    } else if (msg->msgType == JOINREP) {
        cout << memberNode->addr.getAddress() << " Received JOINREP" << endl;
        // cout << "Size of constants" << sizeof(MessageHdr) << " " << sizeof(int) << endl;

        // for(int i=0;i<size;i++) {
        //     cout << (int)*(data+i) << " ";
        // }
        cout << endl;

        memberNode->inGroup = true;

        // Add myself into membership list
        updateMemberList(&memberNode->addr, memberNode->heartbeat);

        // Add introducer into membership list
        // To be improved
        int num = * (int *) (data + sizeof(MessageHdr));
        char *p = data + sizeof(MessageHdr) + sizeof(int);
        // cout << "JOIN REP Has " << num << " entries" << endl;
        for(int i = 0; i < num; i++) {
            Address remoteAddr;
            memcpy(&(remoteAddr.addr), p, addrSize);
            long hb = *(long *) (p + addrSize);

            updateMemberList(&remoteAddr, hb);

            p = p + addrSize + sizeof(long);
        }

        // Introducer may send me more members ??
    } else if (msg->msgType == HEARTBEAT) {
        // process heartbeat
    } else if (msg->msgType == MEMGOSSIP) {
        // process gossip
    }

}

/**
 * FUNCTION NAME: nodeLoopOps
 *
 * DESCRIPTION: Check if any node hasn't responded within a timeout period and then delete
 * 				the nodes
 * 				Propagate your membership list
 */
void MP1Node::nodeLoopOps() {

	/*
	 * Your code goes here
	 */
    // cleanupMemberList();
    // broadcastHeartBeat();
    return;
}

char* MP1Node::makeMemberListMsg(enum MsgTypes type, size_t *size) {
    int memlen = memberNode->memberList.size();
    int addrSize = sizeof(memberNode->addr.addr);

    *size = sizeof(MessageHdr) + sizeof(int) + (addrSize + sizeof(long)) * memlen ;
    char *msg = (char *)malloc(*size);
    ((MessageHdr *) msg) -> msgType = type;
    *(int *)(msg + sizeof(MessageHdr)) = memlen;

    char *p = msg + sizeof(MessageHdr) + sizeof(int);

    vector<MemberListEntry>::iterator it = memberNode->memberList.begin();
    for (; it != memberNode->memberList.end(); ++it) {
        Address remoteAddr = idPort2Address(it->id, it->port);
        memcpy(p, &remoteAddr.addr, addrSize);
        memcpy(p + addrSize, &it->heartbeat, sizeof(long));
        p = p + addrSize + sizeof(long);
    }

    return msg;
}

void MP1Node::broadcastHeartBeat() {
    vector<MemberListEntry>::iterator it = memberNode->memberList.begin();
    int addrSize = sizeof(memberNode->addr.addr);

    for (; it != memberNode->memberList.end(); ++it) {
        if (it->heartbeat >= 0) {
            Address remoteAddr = idPort2Address(it->id, it->port);

            size_t hbSize = sizeof(MessageHdr) + addrSize + sizeof(long);
            char *hb = (char *)malloc(hbSize);
            ((MessageHdr *) hb) -> msgType = HEARTBEAT;
            memcpy(hb + sizeof(MessageHdr), &memberNode->addr.addr, addrSize);
            memcpy(hb + sizeof(MessageHdr) + addrSize, &memberNode->heartbeat, sizeof(long));
            emulNet->ENsend(&memberNode->addr, &remoteAddr, hb, hbSize);
        }
    }
}

void MP1Node::updateMemberList(Address *remote, long hb) {
    vector<MemberListEntry>::iterator it = memberNode->memberList.begin();
    int id = *(int *)(&remote->addr);
    short port = *(short *)(&remote->addr[4]);
    bool isNew = true;
    for (; it != memberNode->memberList.end(); ++it) {
        if(it->id == id && it->port == port) {
            isNew = false;
            if(it -> heartbeat >= 0 && hb > it->heartbeat) {
                it->heartbeat = hb;
                it->timestamp = par->getcurrtime();
            }
        }
    }

    if (isNew) {
        memberNode->memberList.push_back(MemberListEntry(id, port, hb, par->getcurrtime()));
        log->logNodeAdd(&memberNode->addr, remote);
    }
}

void MP1Node::cleanupMemberList() {
    int currtime = par->getcurrtime();
    vector<MemberListEntry>::iterator it = memberNode->memberList.begin();

    while(it != memberNode->memberList.end()){
        if (it->heartbeat == -1 && it->timestamp <= currtime - TREMOVE) {
            it = memberNode->memberList.erase(it);
        } else if(it->heartbeat >= 0 && it->timestamp <= currtime - TFAIL) {
            it->heartbeat = -1;
            ++it;
        } else {
            ++it;
        }
    }
}
/**
 * FUNCTION NAME: isNullAddress
 *
 * DESCRIPTION: Function checks if the address is NULL
 */
int MP1Node::isNullAddress(Address *addr) {
	return (memcmp(addr->addr, NULLADDR, 6) == 0 ? 1 : 0);
}

/**
 * FUNCTION NAME: getJoinAddress
 *
 * DESCRIPTION: Returns the Address of the coordinator
 */
Address MP1Node::getJoinAddress() {
    Address joinaddr;

    memset(&joinaddr, 0, sizeof(Address));
    *(int *)(&joinaddr.addr) = 1;
    *(short *)(&joinaddr.addr[4]) = 0;

    return joinaddr;
}

/**
 * FUNCTION NAME: initMemberListTable
 *
 * DESCRIPTION: Initialize the membership list
 */
void MP1Node::initMemberListTable(Member *memberNode) {
	memberNode->memberList.clear();
}

/**
 * FUNCTION NAME: printAddress
 *
 * DESCRIPTION: Print the Address
 */
void MP1Node::printAddress(Address *addr)
{
    printf("%d.%d.%d.%d:%d \n",  addr->addr[0],addr->addr[1],addr->addr[2],
                                                       addr->addr[3], *(short*)&addr->addr[4]) ;    
}

void MP1Node::printSendMsg(Address *remote, char *msg, int msgsize) {
    cout << memberNode->addr.getAddress() << "Sending to ";
    printAddress(remote);
    for(int i = 0; i < msgsize; i ++){
        cout << *(msg + i) << " ";
    }
    cout << endl;
}

Address MP1Node::idPort2Address(int id, short port) {
    Address remoteAddr;
    *(int *)remoteAddr.addr = id;
    *(short *)(&remoteAddr.addr[4]) = port;
    return remoteAddr;
}