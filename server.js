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
const peers = new Map();

io.on('connection', socket => {
    const id = Math.random().toString(36).substr(2, 9); // generate random ID
    peers.set(id, socket);
    console.log(`[Server] New connection: ${id}`);

    socket.on('join', ({ room, type, to }) => {
        // =========================
        // 공통
        // =========================
        if (!rooms.has(room)) {
            console.log(`[Server] Room ${room} does not exist, creating new room.`);
            rooms.set(room, new Set());
        }
        rooms.get(room).add(id);                 // 핵심

        socket.join(room);

        // 본인 id 전송 0906
        socket.emit('my-id', id);

        // =========================
        // type 분기
        // =========================
        if (type === 'mesh') {
            // Mesh를 위한 로직. 새로운 참가자가 오면, 새로운 참가자는 기존 참가자 목록(existing-peers)을 받아 각각과 연결을 시도하고,
            // 기존 참가자들에게는 new-peer 이벤트를 보내 그들 쪽에서도 연결을 준비하게 함.

            // 기존 참가자 목록 전송  (나 자신 제외)
            const existingPeers = [...peers.keys()].filter(peerId => peerId !== id);
            console.log(`[Server] Existing peers in room ${room}:`, existingPeers);
            socket.emit('existing-peers', existingPeers);

        }

        else if (type === '1_to_n') {
            const existingPeers = [];
            // 맨처음 참가자만 전달  (나 자신 제외)
            if (peers.size > 1) {
                const allKeys = [...peers.keys()]; // 모든 키를 배열로 변환
                const firstKey = allKeys[0];      // 배열의 첫 번째 요소 접근
                console.log(`[Server] First peer in room ${room} `, firstKey);
                existingPeers.push(firstKey);
            }
            else if (peers.size <= 1) {
                console.log(`[Server] No existing peers in room ${room}`);
            }

            console.log(`[Server] Existing peers in room ${room}:`, existingPeers);
            socket.emit('existing-peers', existingPeers);

        }

        else if (type === 'redial') {
            // redial은 특정 대상(to)만 다시 연결 대상으로 내려주도록 구성
            const existingPeers = [];
            if (to) existingPeers.push(to);

            console.log(`[Server] Existing peers in room ${room}:`, existingPeers);
            socket.emit('existing-peers', existingPeers);
        }

        // =========================
        // 공통 
        // =========================
        // 
        if (type !== 'redial') {

            // 디버깅 로그
            console.log(`[room:${room}] join ->`, id);

            // room에 참가한 소켓에 room 정보 저장, data는 기본적으로 {}, 원하는 key 추가가능
            if (!socket.data) socket.data = {};
            socket.data.room = room;
        }
    });


    // offer 전달
    socket.on('offer', ({ to, data }) => {
        const targetSocket = peers.get(to);
        if (!targetSocket) {   // 없으면 여기서 drop
            const from = socket.data?.peerid ?? 'unknown';
            console.warn(`[Server] Drop offer: target missing. from=${from}, to=${to}`);
            return;
        }
        targetSocket.emit('offer', { from: id, data });
        // console.log(`[Server] Offer from ${id} to ${to}`);
    });
    // answer 전달
    socket.on('answer', ({ to, data }) => {
        /// peers 객체에서 to에 해당하는 소켓을 찾음
        const targetSocket = peers.get(to);
        if (!targetSocket) {   // 없으면 여기서 drop
            const from = socket.data?.peerid ?? 'unknown';
            console.warn(`[Server] Drop offer: target missing. from=${from}, to=${to}`);
            return;
        }
        targetSocket.emit('answer', { from: id, data });
        // console.log(`[Server] Answer from ${id} to ${to}`);
    });

    //<kau> After a user received signal info by offer and answer, it sends the its candidate info to the another peer
    socket.on('candidate', ({ to, data }) => {
        const targetSocket = peers.get(to);
        if (!targetSocket) {   // 없으면 여기서 drop
            const from = socket.data?.peerid ?? 'unknown';
            console.warn(`[Server] Drop candidate: target missing. from=${from}, to=${to}`);
            return;
        }
        targetSocket.emit('candidate', { from: id, data });
        // console.log(`[Server] Candidate from ${id} to ${to}`);
    })


    // 추가 구현 했다고 되어있음. 네트워크 연결이 불안정하거나 늦게 붙는 경우를 대비해서 모아둔 candidate들을 한 번에 배열로 보내는 로직을 추가 구현한건가?
    socket.on('candidateArray', ({ to, data }) => {
        const targetSocket = peers.get(to);
        if (!targetSocket) {   // 없으면 여기서 drop
            const from = socket.data?.peerid ?? 'unknown';
            console.warn(`[Server] Drop candidateArray: target missing. from=${from}, to=${to}`);
            return;
        }
        targetSocket.emit('candidateArray', { from: id, data });
        console.log(`[Server] Candidate from ${id} to ${to}`);
    })

    socket.on('disconnect', () => {
        const room = socket.data.room;
        if (room && rooms.has(room)) {
            const peer = rooms.get(room);
            peer.delete(id);
            socket.to(room).emit('disconnected', id);
            console.log(`[Server] Peer ${id} disconnected from room ${room}`);
        }
        peers.delete(id);                        // 핵심
    });
});