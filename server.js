const fs = require('fs');
const https = require('https');
const express = require('express');
// const WebSocket = require('ws');
const path = require('path');

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
// const WebSocket = require('ws');
// const server = new WebSocket.Server({ port: 8000 , host: '0.0.0.0'});

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
// broadcaster 관리용 객체(맵) (key: roomid, value: { broadcasters: {}, allpeers: {}} )
var listOfBroadcasts = {};
const AVAILABLE_BROADCASTING_NUMBER = 1; // 각 중계자가 감당할 수 있는 최대 시청자 수

io.on('connection', socket => {
    let id = Math.random().toString(36).substr(2, 9); // generate random ID
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
        let setid = typeof payload === 'string' ? id : (payload.myid ?? id); // id가 payload에 있으면 사용, 없으면 새로 생성한 id 사용
        console.log(`[Server] join event received. room: ${room}, type: ${type}, id: ${setid}`);

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
        // --------------------------------------------
        // 1-1) listOfBroadcasts의 availableBroadcasters에 id가 존재한다면, 해당 피어에 대한 topology 정보 초기화
        // Hardreset= redial 시 기존 id를 재사용하지 않고, 새로운 id를 발급하도록 처리
        // --------------------------------------------

        // if (listOfBroadcasts[room] && listOfBroadcasts[room].activeBroadcasters[setid]) {
        //     console.log(`[Server] Re-initializing topology info for existing peer ${setid} in room ${room}`);
        //     removePeer(room, setid);
        //     setid = Math.random().toString(36).substr(2, 9); // 새로운 id 할당
        //     id = setid; // 외부에서 참조하는 id도 갱신
        //     console.log(`[Server] New setid & id assigned: ${setid}&&${id}`);
        // }

        // --------------------------------------------
        // 2) type === 'broadcast' (기존 join-broadcast 로직)
        // --------------------------------------------
        if (type === 'broadcast') {
            // Peer type 생성
            /**
             * @type {{peerid: string, socket: any, roomid: string, parentid: string | null, childrenids: string[],
             * isBroadcaster: boolean, isFull: boolean, numberOfViewers: number, active: boolean, session: any}}
             */
            const peer = {
                peerid: setid,
                socket: socket,
                roomid: room,
                parentid: null,          // 부모 노드
                childrenids: [],         // 자신을 부모로 삼는 자식 peerid 목록
                isBroadcaster: false,
                isFull: false,
                numberOfViewers: 0,
                treeLevel: -1,           // root=0, child=1, ...
                active: true,
                session: null
            };

            // room 별 broadcast 구조 초기화
            if (!listOfBroadcasts[peer.roomid]) {
                listOfBroadcasts[peer.roomid] = {
                    broadcasters: {},         // 자식 받을 수 있는 중계자들
                    activeBroadcasters: {},   // isBroadcaster=true 활성 중계자들
                    allpeers: {}              // 방 내 전체 참가자(중계자 포함)
                };
            }

            // 본인 id 전송
            socket.emit('my-id', setid);

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
            } else {
                // Root broadcaster 설정
                console.log('No available broadcaster found, setting as root broadcaster.');
                peer.isBroadcaster = true;
                peer.treeLevel = 0;
                listOfBroadcasts[peer.roomid].activeBroadcasters[peer.peerid] = peer;

                // root는 자기 자신을 parent로 둠(기존 코드 유지)
                peer.parentid = setid;
                socket.emit('root-broadcaster');
            }

            // broadcasters / allpeers 등록
            listOfBroadcasts[peer.roomid].broadcasters[peer.peerid] = peer;
            listOfBroadcasts[peer.roomid].allpeers[peer.peerid] = peer;

            // peers 맵 등록
            peers.set(setid, peer);

            // new-parent 전송 (root면 자기 자신이므로 전송 안 함)
            console.log('id: ', setid, 'Parent id:', peer.parentid);
            if (setid != peer.parentid) socket.emit('new-parent', peer.parentid);
            // 디버깅 로그
            console.log(`[room:${room}] Tree size`, peers.size);

            return; // broadcast 처리 끝
        }

        // --------------------------------------------
        // 3) type === 'redial' (기존 join-redial 로직)
        // --------------------------------------------
        if (type === 'redial') {
            // 기존 peer 가져오기 
            let peer = peers.get(setid);
            if (!peer) {
                console.log(`[Server] No existing peer found for redial with id: ${setid}`);
                return;
            }
            var activeBroadcasters = listOfBroadcasts[peer.roomid].activeBroadcasters;
            // 비활성화된 피어 정리 필요하다면 위치 변경
            for (var activeBroadcaster in activeBroadcasters) {
                console.log(`[Server] Considering peer !: ${activeBroadcaster} for cleanup during redial of peer ${setid}`);
                var targetpeer = activeBroadcasters[activeBroadcaster];
                if (targetpeer.active === false) {
                    console.log(`[Server] Removing inactive peer: ${targetpeer.peerid}`);
                    delete allPeers[targetpeer.peerid];
                    removePeer(targetpeer.roomid, targetpeer.peerid);
                    continue; // 비활성화된 피어는 건너뛰고, allpeers 목록에서 제거
                }
            }
            var newBroadcaster = getFirstAvailableBroadcaster(peer);
            if (newBroadcaster) {
                // Treelevel이 나보다 낮은 해당 부모를 활성 broadcaster로 전환
                if (listOfBroadcasts[peer.roomid].broadcasters[newBroadcaster.peerid].numberOfViewers === 0) {
                    console.log('Setting newBroadcaster ', setid, '==', peer.peerid, 'as true');
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
                console.log(`[Server] No available broadcaster found for redial of peer ${setid} in room ${room}`);
            }
            // broadcasters / allpeers 갱신
            listOfBroadcasts[peer.roomid].broadcasters[peer.peerid] = peer;
            listOfBroadcasts[peer.roomid].allpeers[peer.peerid] = peer;

            // peers 맵 업데이트
            peers.set(setid, peer);

            // Subtree level 재설정
            updateSubtreeLevels(peer.roomid, peer.peerid);

            // new-parent 전송
            console.log('id: ', setid, 'new Parent setid:', peer.parentid);
            if (setid != peer.parentid) socket.emit('new-parent', peer.parentid);

            // 디버깅 로그
            console.log(`[room:${room}] Tree size`, peers.size);

            return; // redial 처리 끝
        }

        // --------------------------------------------
        // 4) 예외: type 값이 이상한 경우
        // --------------------------------------------
        console.log(`[Server] Unknown join type: ${type}`);
    });
    /* 기존 join-redial, join-broadcast 주석 처리
        2025-12-24 통합버전으로 대체됨
    */


    function getFirstAvailableBroadcaster(peer) {
        var broadcasters = listOfBroadcasts[peer.roomid].broadcasters;
        var firstResult;

        for (var broadcasterId in broadcasters) {
            var broadcaster = broadcasters[broadcasterId];
            // console.log(`[Server] Considering broadcaster: ${broadcaster.peerid} for peer: ${peer.peerid}`);
            if (broadcaster.peerid === peer.peerid) {
                continue; // 자기 자신은 건너뜀
            }
            console.log(`[Server] Evaluating broadcaster: ${broadcaster.peerid} at level ${broadcaster.treeLevel}, isFull: ${broadcaster.isFull}`);
            // 1. 후보군 조건 확인 (꽉 차지 않음 && peer보다 상위 레벨)
            if (!broadcaster.isFull) {
                console.log(`[Server] Checking broadcaster: ${broadcaster.peerid} at level ${broadcaster.treeLevel}`);
                if (broadcaster.treeLevel < peer.treeLevel || peer.treeLevel === -1) {
                    // 2. 가장 낮은 treeLevel 선택 로직
                    // 아직 선택된 게 없거나(null), 현재 broadcaster의 레벨이 기존 선택된 것보다 더 낮다면(더 상위라면) 교체
                    if (!firstResult || broadcaster.treeLevel < firstResult.treeLevel) {
                        firstResult = broadcaster;
                        console.log(`[Server] Available broadcaster found: ${broadcaster.peerid} at level ${broadcaster.treeLevel}`);
                    }
                }

            }
            // 3. 조건에 맞지 않는 Broadcaster(꽉 참 등)는 제외
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

        const q = [peerId];
        const visited = new Set([peerId]);

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

                // (선택) 트리 일관성 체크: parent.childrenids 안에 있는데 실제 parentid가 다르면 건너뜀
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
            return;
        }
        targetPeer.socket.emit('offer', { from: id, data });
        // console.log(`[Server] Offer from ${id} to ${to}`);
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
        if (!room || !rooms.has(room)) return;

        const peer = peers.get(id);  // id -> peer 객체
        if (!peer) return;

        peer.active = false;

        console.log(`[Server] Marked peer ${id} as inactive peer`);
    });

    function removePeer(roomid, peerid) {
        // 0) room 유효성 검사
        const room = roomid;
        if (!room || !rooms.has(room)) return;

        // 1) peer 객체 찾기
        const peer = peers.get(peerid);
        if (!peer) return;

        // 2) broadcast 구조 찾기 
        const broadcast = listOfBroadcasts[peer.roomid]; // peer.roomid는 room과 같다고 가정
        if (!broadcast) return;

        console.log('[Server] Handling disconnection of peer ', peer.peerid);

        // 3) 부모의 numberOfViewers 감소 
        if (peer.parentid && peer.parentid !== peer.peerid) {
            const parent = broadcast.allpeers[peer.parentid];
            if (parent) {
                parent.numberOfViewers = Math.max(0, parent.numberOfViewers - 1);

                // 만약 꽉 차서 isFull 이었는데, 다시 자리가 생겼다면 풀어주기
                if (parent.numberOfViewers < AVAILABLE_BROADCASTING_NUMBER) {
                    console.log('[Sever] Setting parent ', parent.peerid, ' as not full');
                    parent.isFull = false;
                    console.log('[Server] Previous broadcasters ', broadcast.broadcasters);

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

        console.log('[Server] Removed peer from broadcasters and allpeers:', broadcast.allpeers);

        // 5) rooms / peers 맵 정리
        const roomSet = rooms.get(room);
        roomSet && roomSet.delete(peerid);
        peers.delete(peerid);

        console.log(`[Server] Peer ${peerid} disconnected from room ${room} Peers size :`, peers.size);
        console.log('[Server] Updated broadcasters ', broadcast.broadcasters);
    }

});