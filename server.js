const fs = require('fs');
const https = require('https');
const express = require('express');
const app = express();

// HTTPS 인증서
const options = {
    key: fs.readFileSync('C:\\Windows\\System32\\key.pem'), // <kau> added to resolve https issues
    cert: fs.readFileSync('C:\\Windows\\System32\\cert.pem')
};

// HTTPS 서버 생성
const httpsServer = https.createServer(options, app);

// 정적 파일 (index.html 등)
app.use(express.static(__dirname));

// WebSocket 서버 붙이기

httpsServer.listen(8000, '0.0.0.0', () => {
    console.log(' HTTPS server running');
});

/* 외부 접속시 https://192.168.0.3:8000 when opening html */

const { Server } = require('socket.io');
const io = new Server(httpsServer, {
    cors: {
        origin: "*",
        methods: ["GET", "POST"]
    }
});

const rooms = new Map();
// 각 방의 lisOfBroadcasts 의 allpeers의 합
const peers = new Map();
// broadcaster 관리용 객체(맵) (key: roomid, value: { broadcasters: {}, activeBroadcasters: {} ,allpeers: {}} )
var listOfBroadcasts = {};
const AVAILABLE_BROADCASTING_NUMBER = 50; // 각 중계자가 감당할 수 있는 최대 시청자 수
// 디버깅용 시퀀스 넘버
let seq = 0;

io.on('connection', socket => {
    let id = Math.random().toString(36).substr(2, 9) + '_' + seq++; // generate random ID
    // 기존의  id-socket 맵핑을 새로운 Peer 객체로 변경
    // peers.set(id, socket);
    console.log(`[Server] New connection: ${id}`);
    /* 수정: join-mesh 로 변경 251224 */
    socket.on('join-mesh', room => {
        if (!rooms.has(room)) {
            console.log(`[Server] Room ${room} does not exist, creating new room.`);
            rooms.set(room, new Set());
        }
        socket.join(room);

        // 본인 id 전송 0906
        socket.emit('my-id', id);

        // 기존 참가자 목록 전송  (나 자신 제외)
        const existingPeers = [...peers.keys()].filter(peerId => peerId !== id);
        console.log(`[Server] Existing peers in room ${room}:`, existingPeers);
        socket.emit('existing-peers', existingPeers);

        // 모두에게 new peer 이벤트 전송 (나 자신 제외)
        socket.to(room).emit('new-peer', id);
        console.log(`[Server] Peer ${id} joined room ${room}`);

        // 디버깅 로그
        console.log(`[room:${room}] join ->`, id);
        // room에 참가한 소켓에 room 정보 저장, data는 기본적으로 {}, 원하는 key 추가가능
        if (!socket.data) socket.data = {};
        socket.data.room = room;

    });

    /**
 * join-broadcast + join-redial 통합 버전
 *
 * 클라이언트에서 아래처럼 호출한다고 가정:
 *   socket.emit('join', { room: 'room1', type: 'broadcast' })
 *   socket.emit('join', { room: 'room1', type: 'redial' })
 *
 * type 값으로 "broadcast" / "redial" 분기
 */
    socket.on('join', (payload) => {
        // --------------------------------------------
        // 0) 파라미터 파싱 (string type으로 분기)
        // socket.emit('join', 'room1') 형태도 지원 
        // --------------------------------------------
        const room = typeof payload === 'string' ? payload : payload.room;
        const type = typeof payload === 'string' ? 'broadcast' : payload.type; // 기본은 broadcast로 취급 가능
        // 1:N의 경우 to라는 payload가 오지만, 통합버전에서는 myid로 대체
        let myid = typeof payload === 'string' ? id : (payload.myid ?? id); // 0:1 d가 payload에 있으면 사용, 없으면 새로 생성한 id 사용
        console.log(`[Server] join event received. room: ${room}, type: ${type}, id: ${myid}`);

        // --------------------------------------------
        // 1) Room join 공통 로직
        // --------------------------------------------
        if (!rooms.has(room)) {
            console.log(`[Server] Room ${room} does not exist, creating new room.`);
            rooms.set(room, new Set());
        } else {
            console.log(`[Server] Room ${room} exists, joining room.`);
        }
        socket.join(room);

        // room에 참가한 소켓에 room 정보 저장 (disconnect에서 사용)
        if (!socket.data) socket.data = {};
        socket.data.room = room;

        // 모든 피어 레벨 별 트리 구조 확인 ( peer.treeLevel 기준 )



        // --------------------------------------------
        // 2) type === 'broadcast' (기존 join-broadcast 로직)
        // --------------------------------------------
        if (type === 'broadcast') {
            // Peer type 생성
            /**
             * @type {{peerid: string, socket: any, roomid: string, parentid: string | null, childrenids: string[],
             * isBroadcaster: boolean, isFull: boolean, numberOfViewers: number, isBroadcaster active: boolean, treeLevel: number, isRoot: boolean}}
             */
            const peer = {
                peerid: myid,
                socket: socket,
                roomid: room,
                parentid: null,          // 부모 노드
                childrenids: [],         // 자신을 부모로 삼는 자식 peerid 목록
                isBroadcaster: false,
                isFull: false,
                numberOfViewers: 0,
                treeLevel: -1,           // root=0, child=1, ...
                active: true,
                isRoot: false
            };

            // room 별 broadcast 구조 초기화
            if (!listOfBroadcasts[peer.roomid]) {
                listOfBroadcasts[peer.roomid] = {
                    broadcasters: {},         // 자식 받을 수 있는 중계자들 , isFull=false인 것들
                    activeBroadcasters: {},   // isBroadcaster=true 활성 중계자들
                    allpeers: {}              // 방 내 전체 참가자(중계자 포함)
                };
            }

            // 비활성화된 피어 정리 active=false 피어 정리
            var tmp_peers = listOfBroadcasts[peer.roomid].allpeers;

            for (var tmp_peer in tmp_peers) {
                console.log(`[Server] Considering peer !: ${tmp_peer} for cleanup during broadcast of peer ${myid}`);
                var targetpeer = tmp_peers[tmp_peer];
                if (targetpeer.active === false) {
                    console.log(`[Server] Removing inactive peer: ${targetpeer.peerid}`);
                    removePeer(targetpeer.roomid, targetpeer.peerid);
                    continue; // 비활성화된 피어는 건너뛰고,peers 목록에서 제거
                }
            }

            // 본인 id 전송
            socket.emit('my-id', myid);

            // 첫 브로드캐스터(부모) 탐색
            var firstBroadcaster = getFirstAvailableBroadcaster(peer);

            if (firstBroadcaster) {
                // 부모가 "처음으로 viewer를 받는 순간" 해당 부모를 활성 broadcaster로 전환
                if (listOfBroadcasts[peer.roomid].broadcasters[firstBroadcaster.peerid].numberOfViewers === 0) {
                    console.log('Setting firstBroadcaster', id, 'as true');
                    peer.isBroadcaster = true;
                    listOfBroadcasts[peer.roomid].activeBroadcasters[peer.peerid] = peer;
                }

                // 부모 viewer 수 증가 + childrenids에 자식 추가
                listOfBroadcasts[peer.roomid].broadcasters[firstBroadcaster.peerid].numberOfViewers++;
                listOfBroadcasts[peer.roomid].broadcasters[firstBroadcaster.peerid].childrenids.push(peer.peerid);

                // 부모/레벨 세팅
                peer.parentid = firstBroadcaster.peerid;
                peer.treeLevel = firstBroadcaster.treeLevel + 1;

                // 부모가 꽉 찼으면 isFull=true
                if (listOfBroadcasts[peer.roomid].broadcasters[firstBroadcaster.peerid].numberOfViewers >= AVAILABLE_BROADCASTING_NUMBER) {
                    listOfBroadcasts[peer.roomid].broadcasters[firstBroadcaster.peerid].isFull = true;
                }
            }
            else {
                // Root broadcaster 설정
                console.log('No available broadcaster found, setting as root broadcaster.');
                peer.isBroadcaster = true;
                peer.treeLevel = 0;
                listOfBroadcasts[peer.roomid].activeBroadcasters[peer.peerid] = peer;

                // root는 자기 자신을 parent로 둠(기존 코드 유지)
                peer.parentid = myid;
                peer.isRoot = true;
                socket.emit('root-broadcaster');
            }

            // broadcasters / allpeers 등록
            listOfBroadcasts[peer.roomid].broadcasters[peer.peerid] = peer;
            listOfBroadcasts[peer.roomid].allpeers[peer.peerid] = peer;

            // peers 맵 등록
            peers.set(myid, peer);

            // new-parent 전송 (root면 자기 자신이므로 전송 안 함)
            console.log('id: ', myid, 'Parent id:', peer.parentid);
            if (myid != peer.parentid) socket.emit('new-parent', peer.parentid);
            // 디버깅 로그
            console.log(`[room:${room}] Tree size by broadcast`, peers.size);

            // 디버깅용 트리 구조 출력
            printTreeStructure(room);

            return; // broadcast 처리 끝
        }

        // --------------------------------------------
        // 3) type === 'redial' 
        // --------------------------------------------
        if (type === 'redial') {
            // myid 기준 나의 peer 객체 가져오기 
            let peer = peers.get(myid);
            if (!peer) {
                console.log(`[Server] No existing peer found for redial with id: ${myid}`);
                return;
            }
            // 비활성화된 피어 정리 active=false 피어 정리
            var tmp_peers = listOfBroadcasts[peer.roomid].allpeers;

            for (var tmp_peer in tmp_peers) {
                console.log(`[Server] Considering peer !: ${tmp_peer} for cleanup during broadcast of peer ${myid}`);
                var targetpeer = tmp_peers[tmp_peer];
                if (targetpeer.active === false) {
                    console.log(`[Server] Removing inactive peer: ${targetpeer.peerid}`);
                    removePeer(targetpeer.roomid, targetpeer.peerid);
                    continue; // 비활성화된 피어는 건너뛰고,peers 목록에서 제거
                }
            }
            var newBroadcaster = getFirstAvailableBroadcaster(peer);
            if (newBroadcaster) {
                // Treelevel이 나보다 낮은 해당 부모를 활성 broadcaster로 전환
                if (listOfBroadcasts[peer.roomid].broadcasters[newBroadcaster.peerid].numberOfViewers === 0) {
                    console.log('Setting newBroadcaster ', myid, '==', peer.peerid, 'as true');
                    peer.isBroadcaster = true;
                    listOfBroadcasts[peer.roomid].activeBroadcasters[peer.peerid] = peer;
                }

                // 부모 viewer 수 증가 + childrenids에 자식 추가
                listOfBroadcasts[peer.roomid].broadcasters[newBroadcaster.peerid].numberOfViewers++;
                listOfBroadcasts[peer.roomid].broadcasters[newBroadcaster.peerid].childrenids.push(peer.peerid);

                // 부모/ 나의 레벨 세팅
                peer.parentid = newBroadcaster.peerid;
                peer.treeLevel = newBroadcaster.treeLevel + 1;

                // 부모가 꽉 찼으면 isFull=true
                if (listOfBroadcasts[peer.roomid].broadcasters[newBroadcaster.peerid].numberOfViewers >= AVAILABLE_BROADCASTING_NUMBER) {
                    listOfBroadcasts[peer.roomid].broadcasters[newBroadcaster.peerid].isFull = true;
                }
            }
            else {
                console.log(`[Server] No available broadcaster found for redial of peer ${myid} in room ${room}`);
            }
            // broadcasters / allpeers 갱신
            listOfBroadcasts[peer.roomid].broadcasters[peer.peerid] = peer;
            listOfBroadcasts[peer.roomid].allpeers[peer.peerid] = peer;

            // setid에 대한 peer를 peers 맵 업데이트
            peers.set(myid, peer);

            // Subtree level 재설정
            updateSubtreeLevels(peer.roomid, peer.peerid);

            // new-parent 전송
            console.log('id: ', myid, 'new Parent setid:', peer.parentid);
            if (myid != peer.parentid) socket.emit('new-parent', peer.parentid);

            // 디버깅 로그
            console.log(`[room:${room}] Tree size by redial`, peers.size);

            // 디버깅용 트리 구조 출력
            printTreeStructure(room);

            return; // redial 처리 끝
        }

        // --------------------------------------------
        // 4) 예외: type 값이 이상한 경우
        // --------------------------------------------
        // console.log(`[Server] Unknown join type: ${type}`);

    });
    /* 기존 join-redial, join-broadcast 주석 처리
        2025-12-24 통합버전으로 대체됨
    */

    /* Tree 구조 디버깅용 */
    function printTreeStructure(roomid) {
        const broadcast = listOfBroadcasts[roomid];
        if (!broadcast) return;

        const allpeers = broadcast.allpeers;
        const levelMap = {};

        // 레벨별로 피어 분류
        for (const peerid in allpeers) {
            const peer = allpeers[peerid];
            const level = peer.treeLevel;
            if (!levelMap[level]) {
                levelMap[level] = [];
            }
            levelMap[level].push(peer);
        }

        // Key인 레벨 순서대로 출력
        /* 오름차순 */
        const sortedLevels = Object.keys(levelMap).sort((a, b) => a - b);

        let output = '';
        for (const level of sortedLevels) {
            output += `---LV${level}----\n`;
            const peers = levelMap[level];
            const peerStrings = peers.map(p => {
                if (p.isRoot) {
                    return p.peerid;
                } else {
                    return `${p.peerid}(${p.parentid})`;
                }
            });
            output += peerStrings.join(' ') + '\n';
        }

        console.log('\n========== TREE STRUCTURE [' + roomid + '] ==========\n' + output);
    }

    function getFirstAvailableBroadcaster(peer) {
        var broadcasters = listOfBroadcasts[peer.roomid].broadcasters;
        var firstResult;

        for (var broadcasterId in broadcasters) {
            var broadcaster = broadcasters[broadcasterId];
            // console.log(`[Server] Considering broadcaster: ${broadcaster.peerid} for peer: ${peer.peerid}`);
            if (broadcaster.peerid === peer.peerid || !broadcaster.active) {
                continue; // 자기 자신과 !active 건너뜀
            }
            // console.log(`[Server] Evaluating broadcaster: ${broadcaster.peerid} at level ${broadcaster.treeLevel}, isFull: ${broadcaster.isFull}`);
            // 1. 후보군 조건 확인 (꽉 차지 않음 && peer보다 상위 레벨)
            if (!broadcaster.isFull) {
                // console.log(`[Server] Checking broadcaster: ${broadcaster.peerid} at level ${broadcaster.treeLevel}`);
                if (broadcaster.treeLevel < peer.treeLevel || peer.treeLevel === -1) {
                    // 2. 가장 낮은 treeLevel 선택 로직
                    // 아직 선택된 게 없거나(null), 현재 broadcaster의 레벨이 기존 선택된 것보다 더 낮다면(더 상위라면) 교체
                    if (!firstResult || broadcaster.treeLevel < firstResult.treeLevel) {
                        firstResult = broadcaster;
                        // console.log(`[Server] Available broadcaster found: ${broadcaster.peerid} at level ${broadcaster.treeLevel}`);
                    }
                }

            }
            // 3. 조건에 맞지 않는 Broadcaster(꽉 참 등)는 broadcasters에서 제거
            else {
                delete listOfBroadcasts[peer.roomid].broadcasters[broadcasterId];
            }
        }

        return firstResult;
    }

    function updateSubtreeLevels(roomid, peerId) {
        /* - BFS(너비우선탐색)으로 root에서 아래로 내려가며,
*   child.treeLevel = parent.treeLevel + 1 을 적용한다.
*
* @param {string} roomid       - 방 ID (listOfBroadcasts의 key)
* @param {string} peerId   - "레벨을 기준으로 삼을" 서브트리의 루트 peerid
*/
        // broadcast && targetpeer 존재 확인
        const broadcast = listOfBroadcasts[roomid];
        if (!broadcast) return;

        const targetPeer = broadcast.allpeers[peerId];
        if (!targetPeer) return;
        // q: BFS용 큐, visited: 방문 처리용 집합
        const q = [peerId];
        const visited = new Set([peerId]);
        console.log(`[Server-Subtree] Updating subtree levels starting from peer ${peerId} the queue ${q} `);
        while (q.length) {
            const pid = q.shift(); // 현재 부모로 취급할 노드 peerid를 큐에서 꺼냄
            const parent = broadcast.allpeers[pid]; // pid에 해당하는 peer 객체(부모)를 가져온다.
            if (!parent) continue;

            const parentLevel = parent.treeLevel;
            const children = parent.childrenids || [];

            for (const childId of children) {
                if (visited.has(childId)) continue;

                const child = broadcast.allpeers[childId];
                if (!child) continue;

                // 트리 일관성 체크: parent.childrenids 안에 있는데 실제 parentid가 다르면 건너뜀
                if (child.parentid !== parent.peerid) continue;

                child.treeLevel = parentLevel + 1;
                console.log(`[Server] Updated treeLevel of peer ${child.peerid} to ${child.treeLevel} (parent: ${parent.peerid} at level ${parentLevel})`);
                // 방문 처리 및 다음 BFS 대상으로 큐에 넣기
                visited.add(childId);
                q.push(childId);
            }
        }
    }

    socket.on('offer', ({ to, data }) => {
        const targetPeer = peers.get(to);
        if (!targetPeer || !targetPeer.socket || !targetPeer.active) {   // 없으면 여기서 drop
            const from = socket.data?.peerid ?? 'unknown';
            console.warn(`[Server] Drop offer: target missing. from=${from}, to=${to}`);
            // 보낸 피어로 다시 알림 전송
            socket.emit('droppedOffer-redial', { from: id, to });
            return;
        }
        targetPeer.socket.emit('offer', { from: id, data });
        console.log(`[Server] Offer from ${id} to ${to}`);
    });
    socket.on('offer-renegotiate', ({ to, data }) => {
        const targetPeer = peers.get(to);
        if (!targetPeer || !targetPeer.socket || !targetPeer.active) {   // 없으면 여기서 drop
            const from = socket.data?.peerid ?? 'unknown';
            console.warn(`[Server] Drop offer-renegotiate: target missing. from=${from}, to=${to}`);
            return;
        }
        targetPeer.socket.emit('offer', { from: id, data });
        console.log(`[Server] Offer from ${id} to ${to}`);
    });
    //<kau> Send an answer to the peer sent an offer
    socket.on('answer', ({ to, data }) => {
        /// peers 객체에서 to에 해당하는 소켓을 찾음
        const targetPeer = peers.get(to);
        if (!targetPeer || !targetPeer.socket || !targetPeer.active) {   // 없으면 여기서 drop
            const from = socket.data?.peerid ?? 'unknown';
            console.warn(`[Server] Drop offer: target missing. from=${from}, to=${to}`);
            return;
        }
        targetPeer.socket.emit('answer', { from: id, data });
        // console.log(`[Server] Answer from ${id} to ${to}`);
    });
    //<kau> After a peer received signal info by offer and answer, it sends the its candidate info to the another peer
    socket.on('candidate', ({ to, data }) => {
        const targetPeer = peers.get(to);
        if (!targetPeer || !targetPeer.socket || !targetPeer.active) {   // 없으면 여기서 drop
            const from = socket.data?.peerid ?? 'unknown';
            console.warn(`[Server] Drop candidate: target missing. from=${from}, to=${to}`);
            return;
        }
        targetPeer.socket.emit('candidate', { from: id, data });
        // console.log(`[Server] Candidate from ${id} to ${to}`);
    })

    // 1029 새로 추가
    socket.on('candidateArray', ({ to, data }) => {
        const targetPeer = peers.get(to);
        if (!targetPeer || !targetPeer.socket || !targetPeer.active) {   // 없으면 여기서 drop
            const from = socket.data?.peerid ?? 'unknown';
            console.warn(`[Server] Drop candidateArray: target missing. from=${from}, to=${to}`);
            return;
        }
        targetPeer.socket.emit('candidateArray', { from: id, data });
        console.log(`[Server] Candidate from ${id} to ${to}`);
    })

    socket.on('disconnect', () => {
        console.log(`[Server] Peer ${id} disconnected.`);
        const room = socket.data.room;
        if (!room || !rooms.has(room)) {
            console.log(`[Server] Invalid room on disconnect_reset: ${room}`);
            return;
        }


        const peer = peers.get(id);  // id -> peer 객체
        if (!peer) {
            console.log(`[Server] No peer found with id ${id} on disconnect_reset`);
            return;
        }
        // 끊긴 피어 비활성화 처리
        peer.active = false;
        // delete broadcast.activeBroadcasters[peer.peerid];
        if (peer.isRoot) {
            console.log(`[Server] Root peer ${id} disconnected, marking all peers in room ${room} as inactive.`);

            // Room 내 모든 피어 강퇴처리 socket emit('force-disconnect-room')
            // io room socket을 통해 room에 있는 모든 소켓에 force-disconnect-room  이벤트 전송하도록 수정
            io.to(room).emit('force-disconnect-room');

            // listOfBroadcasts[peer.roomid]에 있는 모든 Peer inactive 처리
            const broadcastOflist = listOfBroadcasts[peer.roomid];
            for (const pId in broadcastOflist.allpeers) {
                const p = broadcastOflist.allpeers[pId];
                p.active = false;
            }
            // 260106 수정
            // delete listOfBroadcasts[peer.roomid];
        }
        console.log(`[Server] Marked peer ${id} as inactive peer`);
    });
    // reset 용도 disconnect 별도 구현
    socket.on('disconnect_reset', () => {
        console.log(`[Server] Peer ${id} RESET.`);
        const room = socket.data.room;
        if (!room || !rooms.has(room)) {
            console.log(`[Server] Invalid room on disconnect_reset: ${room}`);
            return;
        }


        const peer = peers.get(id);  // id -> peer 객체
        if (!peer) {
            console.log(`[Server] No peer found with id ${id} on disconnect_reset`);
            return;
        }

        // 끊긴 피어 비활성화 처리
        peer.active = false;
        // delete broadcast.activeBroadcasters[peer.peerid];
        if (peer.isRoot) {
            console.log(`[Server] Root peer ${id} disconnected, marking all peers in room ${room} as inactive.`);

            // Room 내 모든 피어 강퇴처리 socket emit('force-disconnect-room')
            // io room socket을 통해 room에 있는 모든 소켓에 force-disconnect-room  이벤트 전송하도록 수정
            io.to(room).emit('force-disconnect-room');

            // listOfBroadcasts[peer.roomid]에 있는 모든 Peer inactive 처리
            const broadcastOflist = listOfBroadcasts[peer.roomid];
            for (const pId in broadcastOflist.allpeers) {
                const p = broadcastOflist.allpeers[pId];
                p.active = false;
            }

        }
        console.log(`[Server] Marked peer ${id} as inactive peer`);
    });

    /*
    listOfBroadcasts 가 존재할 때, 부모를 받아오기 전 available하지 않은 peer 제거
    */
    function removePeer(roomid, peerid) {
        // 0) room 유효성 검사
        const room = roomid;
        if (!room || !rooms.has(room)) {
            console.log('[Server] Invalid room on removePeer:', room);
            return;
        }

        // 1) peer 객체 찾기
        const peer = peers.get(peerid);
        if (!peer) {
            console.log('[Server] No peer found with id ', peerid);
            return;
        }

        // 2) broadcast 구조 찾기 
        const broadcast = listOfBroadcasts[peer.roomid]; // peer.roomid는 room과 같다고 가정
        if (!broadcast) {
            console.log('[Server] No broadcast structure found for room ', peer.roomid);
            return;
        }

        console.log('[Server] Handling disconnection of peer ', peer.peerid);

        // 3) 부모의 numberOfViewers 감소 
        if (peer.parentid && peer.parentid !== peer.peerid) {
            const parent = broadcast.allpeers[peer.parentid];
            if (parent) {
                parent.numberOfViewers = Math.max(0, parent.numberOfViewers - 1);

                // 만약 꽉 차서 isFull 이었는데, 다시 자리가 생겼다면 풀어주기
                if (parent.numberOfViewers < AVAILABLE_BROADCASTING_NUMBER) {
                    // console.log('[Sever] Setting parent ', parent.peerid, ' as not full');
                    parent.isFull = false;
                    // console.log('[Server] Previous broadcasters ', broadcast.broadcasters);

                    // parent를 다시 broadcasters 후보군에 넣어줌 
                    listOfBroadcasts[peer.roomid].broadcasters[parent.peerid] = parent;
                }

                // parent.childrenids 에서 이 peerid 제거
                parent.childrenids = parent.childrenids.filter(childId => childId !== peer.peerid);
            }
        }

        // 4) 이 피어를 broadcasters/activeBroadcasters/allpeers에서 제거 
        delete broadcast.broadcasters[peer.peerid];
        delete broadcast.activeBroadcasters[peer.peerid];
        delete broadcast.allpeers[peer.peerid];

        console.log('[Server] Removed peer from broadcasters and allpeers:');

        // 5) rooms / peers 맵 정리
        const roomSet = rooms.get(room);
        roomSet && roomSet.delete(peerid);
        peers.delete(peerid);

        console.log(`[Server] Peer ${peerid} disconnected from room ${room} Peers size :`, peers.size);
        // console.log('[Server] Updated broadcasters ', broadcast.broadcasters);
    }

});



