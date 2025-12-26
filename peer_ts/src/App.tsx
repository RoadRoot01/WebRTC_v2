/*
    WebRTC Peer (React + TypeScript)
    1. Candidate 개별 전송
    2. Candidate 개별 수신 및 처리
    ++ 연결 실패시 Candidate 배열로 송수신 받아 Loop문으로 addIceCandidate 처리
    3. 화면 공유 스트림 교체 기능
    4. 비트레이트 설정 기능
    5. SFU / MESH 모드 선택 기능

*/

import React, { use, useCallback, useEffect, useRef, useState } from 'react';
// import logo from './logo.svg';
import './App.css';
import io from 'socket.io-client';
import Video from './component/remoteVideo';

export interface WebRTCUser {
    id: string;
    socket: SocketIOClient.Socket;
    stream: MediaStream;
}

type BitrateLevel = 'min' | 'medium' | 'max';
const BitrateConfig: Record<BitrateLevel, number> = {
    min: 50,   // 50 bps
    medium: 1000, // 1 Mbps
    max: 2000,  // 2 Mbps
};
const displayMediaOptions = {
    video: {
        displaySurface: "monitor", // browser 브라우저 탭 우선적으로 선택
    },
    audio: {
        suppressLocalAudioPlayback: false, // 로컬 오디오 재생 억제 여부
    },
    preferCurrentTab: false, // 현재 탭을 우선적으로 선택
    selfBrowserSurface: "exclude", // 브라우저 자체 화면 제외
    systemAudio: "include", // 시스템 오디오 포함
    surfaceSwitching: "include", // 화면 전환 허용
    monitorTypeSurfaces: "include", // 모니터 유형 화면 포함
};

// 운영 모드 설정
const MODE: string = 'MESH'; // 'MESH' or 'SFU'

// 소켓 인스턴스를 컴포넌트 외부에서 한 번만 생성하여 재렌더링 시 재생성을 방지합니다.
//https://192.168.0.6:8000
export const SIGNALING_SERVER_URL = `https://172.30.1.76:8000`

// const socket = io(`https://192.168.0.8:8000`, { autoConnect: false });
const pcConfig: RTCConfiguration = {
    iceServers: [
        {
            urls: [
                'stun:stun.l.google.com:19302',
                'stun:stun1.l.google.com:19302',
                'stun:stun2.l.google.com:19302',
                'stun:stun3.l.google.com:19302',
                'stun:stun4.l.google.com:19302',
                'stun:23.21.150.121:3478',

            ]
        },
    ]
};
const constraints = {
    video: {

        width: { ideal: 2880, max: 2880 },
        height: { ideal: 1800, max: 1800 },
        frameRate: { ideal: 30, max: 30 },
    },
    audio: true
};
// const pcConfig : RTCConfiguration = {"iceServers":[]};
function App() {
    console.log('Rendering... ');
    let changeCount = 0;
    const room = 'testRoom'; // Example room name

    // Record<<K,T> : TS utility type

    /* UseRef 사용하지 않으면 랜더링시 초기화 문제 발생 */
    const socketRef = useRef<SocketIOClient.Socket | null>(null);
    const pcsRef = useRef<Record<string, RTCPeerConnection>>({});
    /* Others candidates against me */
    const pendingCandRef = useRef<Record<string, RTCIceCandidate[]>>({});
    /* My candidates against others */
    const iceCandidateGatheredArrayRef = useRef<Record<string, RTCIceCandidate[]>>({});
    /* Peer connection types (e.g., 'recvonly' | 'sendonly') */
    const pcTypesRef = useRef<Record<string, string>>({});
    /* hardReset ref to allow calling the useCallback-defined hardReset from handlers
        that are created earlier (createPeerConnection captures this ref). */
    const hardResetRef = useRef<((peerid: string) => Promise<void>) | null>(null);
    // pc Close Test 용 버튼 
    const forceDisconnectPeerRef = useRef<(peerId: string) => boolean>(() => false);

    // const pcRef = useRef<RTCPeerConnection>(null);
    const localStreamRef = useRef<MediaStream>(null);
    const localVideoRef = useRef<HTMLVideoElement>(null);
    const myidRef = useRef<string>('');
    const localStreamSortRef = useRef<string>('userMedia');
    const hasStreamChangedRef = useRef<boolean>(false);
    // user 상태 관리
    const [users, setUsers] = useState<WebRTCUser[]>([]);
    const [myid, setMyid] = useState<string>('');


    const setVideoBitrate = useCallback(async (peerId: string, bitrate: number) => {
        const pc = pcsRef.current[peerId];
        if (!pc) {
            console.error(`[Peer] PeerConnection for ${peerId} not found.`);
            return;
        }

        const senders = pc.getSenders();
        const videoSender = senders.find(sender => sender.track?.kind === 'video');

        if (videoSender) {
            try {
                const parameters = videoSender.getParameters();
                console.log(`[Peer] ${peerId} senderParameters1 : `, parameters);
                if (!parameters.encodings || parameters.encodings.length === 0) {
                    parameters.encodings = [{}];
                    console.log('[Peer] ${peerId} senderParameters2 : ', parameters);
                }
                // 비트레이트 설정
                parameters.encodings[0].maxBitrate = bitrate;

                await videoSender.setParameters(parameters);
                console.log(`[Peer] Video bitrate for ${peerId} set to ${bitrate / 1000}bps.`);
            } catch (e) {
                console.error(`[Peer] Failed to set video bitrate for ${peerId}:`, e);
            }
        } else {
            console.warn(`[Peer] No video sender found for ${peerId}.`);
        }
    }, []);

    /**
 * SDP에서 특정 미디어 타입(예: 'audio', 'video')의 최대 대역폭을 설정
 * SDP를 받은 Peer는 이 값을 참고하여 해당 미디어 스트림의 대역폭을 제한하여 송신
 * Bandwidth Attribute (b=)는 RFC 4566에 따라 설정
 * 'AS' (Application Specific) 타입은 RTP 세션 대역폭
 *
 * @param sdp 원본 Session Description Protocol 문자열.
 * @param mediaType 대역폭을 설정할 미디어 타입 ('audio', 'video' 등).
 * @param maxbps 설정할 최대 대역폭 값 (Kilobits per second).
 * @returns 수정된 SDP 문자열.
 */
    function setMaxBandwidth(sdp: string, mediaType: string, maxbps: number): string {
        console.log(`[SDP] Setting max bandwidth for ${mediaType} to ${maxbps} bps`);
        if (!sdp) {
            console.warn('[SDP] No SDP provided');
            return sdp;
        }
        const lines: string[] = sdp.split('\r\n');
        const newSdp: string[] = [];

        // 미디어 타입에 m= 접두사를 붙여 정확히 일치하는 패턴
        const mediaPattern = `m=${mediaType}`;
        let insideTargetMediaSection: boolean = false;

        for (const line of lines) {
            // 현재 라인을 먼저 새 SDP에 추가
            newSdp.push(line);

            if (line.startsWith('m=')) {
                // 새 미디어 섹션이 시작되면 플래그를 재설정
                insideTargetMediaSection = line.startsWith(mediaPattern);

                // 타겟 미디어 섹션을 찾았고, 이전에 b= 라인을 추가하지 않았다면 삽입
                // b=AS:{maxbps} 라인을 m= 바로 뒤에 추가
                if (insideTargetMediaSection) {
                    // RFC 4566에서 'AS' 타입은 애플리케이션 특정 최대 대역폭을 의미
                    newSdp.push(`b=AS:${maxbps}`);

                    // 삽입 후 플래그를 다시 false로 설정하여 현재 섹션에 b= 라인이 더 이상 추가되지 않도록 함
                    // 다음 m= 라인이 나올 때까지는 새로운 b= 라인을 삽입할 필요가 없음
                    insideTargetMediaSection = false;
                }
            }
            // m= 라인이 아닌 다른 라인(c=, i=, a= 등)에 대해서는 별다른 처리를 하지 않고 넘어감
        }

        return newSdp.join('\r\n');
    }

    /* 사용 예시: 
    const originalSdp = `v=0
    o=- 3795556209 1 IN IP4 127.0.0.1
    s=-
    t=0 0
    a=group:BUNDLE audio video
    m=audio 9 UDP/TLS/RTP/SAVPF 111
    a=mid:audio
    m=video 9 UDP/TLS/RTP/SAVPF 96 97
    a=mid:video`;
    
    const newSdp = setMaxBandwidth(originalSdp, 'video', 512);
    console.log(newSdp);
    */

    // useCallback을 사용하여 getLocalStream 함수를 메모이제이션
    const getLocalStream = useCallback(async () => {
        try {
            console.log('getLocalStream....');
            // 추후 localStreamRef로 로컬 비디오 컴포넌트에서 사용

            localStreamRef.current = await navigator.mediaDevices.getDisplayMedia(constraints);

            // localStreamRef.current = await navigator.mediaDevices.getUserMedia(constraints);

            if (localVideoRef.current) {
                localVideoRef.current.srcObject = localStreamRef.current;
            }
        }
        catch (error) {
            console.error('Error accessing media devices.', error);
        }
    }, []);

    /* 스트림 교체 함수 */
    const changeStream = useCallback(async () => {



        if (localStreamSortRef.current === 'userMedia') {
            console.log(`[Peer] Current stream is not a display source. Changing stream...`);
            localStreamRef.current = await navigator.mediaDevices.getDisplayMedia(constraints);

            // localStreamRef.current = (await navigator.mediaDevices.getUserMedia({ video: true, audio: true }));

            if (localVideoRef.current) {
                localVideoRef.current.srcObject = localStreamRef.current;
            }
            // 모든 피어에 대해 트랙을 교체
            // Object.values() : pcsRef.current 객체의 값들(즉, RTCPeerConnection 인스턴스들)을 배열로 반환
            Object.values(pcsRef.current).forEach(pc => {
                // pc에서 내보내는 트랙들을 가져옴
                const senders = pc.getSenders();
                localStreamRef.current!.getTracks().forEach(track => {
                    // localStreamRef의 각 트랙에 대해, 동일한 종류(kind)의 트랙을 보내는 송신자(sender)를 찾음
                    const sender = senders.find(s => s.track?.kind === track.kind);
                    if (sender) {
                        sender.replaceTrack(track);
                    }
                });
            });
        } else {
            console.log(`[Peer] Current stream is already a display source. Skipping changeStream.`);
        }

        localStreamSortRef.current = 'displayMedia';

    }, []);


    // 비디오 컴포넌트 이외는 한 번만 렌더링
    useEffect(() => {
        socketRef.current = io.connect(SIGNALING_SERVER_URL, { autoConnect: false });
        // console.log('UserMode:', MODE);
        // getLocalStream();
        // 수동으로 연결 시작
        socketRef.current?.connect();
        console.log('Local stream obtained:', localStreamRef.current);


        socketRef.current.on('connect', () => {
            console.log('[Peer] Connected to signaling server');
            // if (MODE === 'SFU') {
            //     console.log('[Peer] Joining room in SFU mode:', room);
            //     socketRef.current?.emit('join-sfu', room);
            // }
            // else if (MODE === 'MESH') {
            //     console.log('[Peer] Joining room in MESH mode:', room);
            //     socketRef.current?.emit('join-mesh', room);
            // }
            socketRef.current?.emit('join', { room: room, type: 'broadcast' });
        });
        socketRef.current.on('root-broadcaster', () => {
            console.log('[Peer] I am the root broadcaster in the room.');
            getLocalStream();
            // 나머지 피어들은 remoteSteram을 받아 localStreamRef.current에 집어넣음
        });
        socketRef.current.on('my-id', (id: string) => {
            console.log('[Peer] My ID:', id);
            myidRef.current = id;
            setMyid(id);
            console.log('My ID set to state:', myidRef.current);
        });
        socketRef.current.on('new-parent', async (parentid: string) => {
            console.log('[Peer] my Parent in room:', parentid);

            const pc = createPeerConnection(parentid, 'recvonly');
            // Store the peer connection in the ref
            pcsRef.current[parentid] = pc;

            // 보내기 전 Bit rate 설정
            // setVideoBitrate(peerid, BitrateConfig.min)

            const offer = await pc.createOffer();
            const newSdp = setMaxBandwidth(offer.sdp || '', 'video', 512000); // 비디오 대역폭을 512bps로 설정
            await pc.setLocalDescription(newSdp ? { type: offer.type, sdp: newSdp } : offer);
            socketRef.current?.emit('offer', { to: parentid, data: newSdp ? { type: offer.type, sdp: newSdp } : offer });

            console.log(`[Peer] Sent Offer to ${parentid}`, newSdp ? { type: offer.type, sdp: newSdp } : offer);
        });


        socketRef.current.on('offer', async ({ from, data }: { from: string, data: any }) => {
            console.log(`[Peer] Received offer from ${from}`, data);

            pcsRef.current[from] = createPeerConnection(from, 'sendonly');
            const pc = pcsRef.current[from];
            // 보내기 전 Bit rate 설정
            // setVideoBitrate(from, BitrateConfig.min)

            await pc.setRemoteDescription(new RTCSessionDescription(data));

            const answer = await pc.createAnswer();
            const newSdp = setMaxBandwidth(answer.sdp || '', 'video', 512000); // 비디오 대역폭 설정
            await pc.setLocalDescription(newSdp ? { type: answer.type, sdp: newSdp } : answer);
            // await pc.setLocalDescription(answer);
            // socketRef.current?.emit('answer', { to: from, data: answer });
            socketRef.current?.emit('answer', { to: from, data: newSdp ? { type: answer.type, sdp: newSdp } : answer });
            // console.log(`[Peer] Sent Answer to ${from}`,answer);
            console.log(`[Peer] Sent Answer to ${from}`, newSdp ? { type: answer.type, sdp: newSdp } : answer);
        });

        socketRef.current.on('answer', async ({ from, data }: { from: string, data: any }) => {
            const pc = pcsRef.current[from];
            if (!pc) {
                console.error('RTCPeerConnection is not initialized.');
                return;
            }
            console.log(`[Peer] Received answer.${from}`, data);
            await pc.setRemoteDescription(new RTCSessionDescription(data));
        });
        // 배열 수신
        socketRef.current.on('candidateArray', async ({ from, data }: { from: string, data: any }) => {
            const pc = pcsRef.current[from];
            if (pc) {
                console.log('[Peer/Test] ICE candidate Array:', data);
                const rd = pc.remoteDescription
                if (!rd) {
                    console.log("[Peer/Test] pc's remoteDescription is Null");
                    // data is cadidateArray
                    pendingCandRef.current[from] = data;
                    return;
                } else {
                    console.log("[Peer/Test] remoteDescription detected. ICECandidate is added");

                    // console.log("[Peer] Pending queue: ", queue);
                    if (data) {
                        console.log("[Peer] pending candidates...");
                        for (const cand of data) {
                            try {
                                await pc.addIceCandidate(cand);
                            } catch (e) {
                                console.warn('[Peer] addIcecandidate failed', e);
                            }
                        }
                    }

                    // await pc.addIceCandidate(new RTCIceCandidate(data.candidate));
                }
                // await pc.addIceCandidate(new RTCIceCandidate(data.candidate));
            }
        });
        // 개별 수신
        socketRef.current.on('candidate', async ({ from, data }: { from: string, data: any }) => {
            const pc = pcsRef.current[from];
            if (pc) {
                console.log('[Peer/Test] ICE candidate event:', data.candidate);
                const rd = pc.remoteDescription
                if (!rd) {
                    console.log("[Peer/Test] pc's remoteDescription is Null");

                } else {
                    console.log("[Peer/Test] remoteDescription detected. ICECandidate is added");
                }
                await pc.addIceCandidate(new RTCIceCandidate(data.candidate));
            }
        });


        socketRef.current.on('disconnect', (peerId: string) => {
            console.log(`[Peer] Peer ${peerId} disconnected.`);
            // if (pcsRef.current[peerId]) {
            //     socketRef.current?.emit('disconnect');
            //     pcsRef.current[peerId].close();
            //     delete pcsRef.current[peerId];
            //     setUsers(prev => prev.filter(u => u.id !== peerId));
            // }

        });

        return () => {
            if (socketRef.current) {
                socketRef.current.disconnect();
            }
            Object.keys(pcsRef.current).forEach((key) => {
                pcsRef.current[key].close();
                delete pcsRef.current[key];
            });
        };

    }, []);

    // useCallback을 사용하여 createPeerConnection 함수를 메모이제이션
    // peerId - parameter, RTCPeerConnection - return type
    const createPeerConnection = useCallback((peerId: string, type: string): RTCPeerConnection => {


        console.log(`[Peer] createPeerConnection ${peerId}`);
        // record the type of this peer connection so handlers can decide actions later
        pcTypesRef.current[peerId] = type;
        const pc = new RTCPeerConnection(pcConfig);
        // const pc = new RTCPeerConnection(config);
        if (type === 'recvonly') {
            console.log(`[Peer] Setting up recvonly connection `);
            pc.addTransceiver('video', { direction: 'recvonly' });
            pc.addTransceiver('audio', { direction: 'recvonly' });
        }
        else {
            if (localStreamRef.current !== null) {
                console.log('[Peer] Add local stream to peer connection');
                localStreamRef.current.getTracks().forEach(track => {
                    // Scalable Mode: track에 다른 피어로부터 받은 Stream 
                    // (내 스트림이 아닌 전달할 Stream을 담으면 됨)
                    pc.addTrack(track, localStreamRef.current!);
                });
            } else {
                console.error('Local media stream is null');
            }

        }


        pc.onicecandidate = event => {
            // console.log('[Peer] ICE candidate event:', event);
            const socket = socketRef.current;
            if (iceCandidateGatheredArrayRef.current[peerId] === undefined) {
                iceCandidateGatheredArrayRef.current[peerId] = [];
            }
            if (event.candidate && socket != null) {
                socket.emit('candidate', { to: peerId, data: { type: 'candidate', candidate: event.candidate } });
                iceCandidateGatheredArrayRef.current[peerId].push(event.candidate);
                // console.log(`[Peer] ICE candidate gathering state: ${pc.iceGatheringState}`);
                // console.log(`[Peer] ICE candidate gathered`);
                // console.log('[Peer] Sending ICE candidate...', event.candidate);
                // socketRef.current?.emit('candidate', { to: peerId, data: { type: 'candidate', candidate: event.candidate } });
            }
        };

        pc.onicecandidateerror = (e) => {
            const err = e as RTCPeerConnectionIceErrorEvent;
            console.warn('ICE error', err.errorCode, err.errorText, err.url);
        };


        pc.onconnectionstatechange = async () => {
            console.log(`[${peerId}] state:`, pc.connectionState);

            if (pc.connectionState === 'disconnected' || pc.connectionState === 'closed') {
                console.log(`[${peerId}] Connection ${pc.connectionState}.`);
                const pcType = pcTypesRef.current[peerId];
                // 재연결 시도 join-redial for recvonly connections
                if (pcType === 'recvonly') {
                    const targetPc = pcsRef.current[peerId];
                    console.log(`[${peerId}] recvonly connection lost. Attempting redial.`);
                    if (targetPc) {
                        // targetPc.close();
                        // delete pcsRef.current[peerId];
                    }
                    // delete pcTypesRef.current[peerId];
                    pendingCandRef.current[peerId] = [];// candidate flush
                    // setUsers(prev => prev.filter(u => u.id !== peerId));

                    renegotiateSamePc(peerId);


                }
            }
            else if (pc.connectionState === 'failed') {
                // 재연결 시도 하드리셋
                // console.log(`[${peerId}] Connection failed. Attempting to restart ICE...`);
                const pcType = pcTypesRef.current[peerId];
                if (pcType === 'recvonly') {
                    try {
                        const targetPc = pcsRef.current[peerId];
                        console.log(`[${peerId}] recvonly connection lost. Attempting redial.`);
                        if (targetPc) {
                            targetPc.close();
                            delete pcsRef.current[peerId];
                            delete pcTypesRef.current[peerId];
                            pendingCandRef.current[peerId] = [];
                            setUsers(prev => prev.filter(u => u.id !== peerId));
                        }
                        else {
                            console.log(`[${peerId}] No existing peer connection found for hard reset.`);
                        }
                        // console.log(`[${peerId}] recvonly connection failed. Attempting hard reset.`);
                        // candidate flush

                        // console.log(`[${peerId}] Soft Resetting...`);                    // console.log(`[${peerId}] Hard Resetting...`);
                        // await softReset(peerId);
                        // candidate flush
                        console.log(`[${peerId}] Hard Resetting...`);
                        socketRef.current?.emit('join', { room: room, type: 'redial' });
                        // await hardReset(peerId);
                        /* 1029 추가 : 연결 실패 했을 때 담아두었던 candidate배열 다시 전송*/
                        // sendIceCandidate(peerId);
                    }
                    catch (e) {
                        // hardReset
                        console.error(e);
                    }
                }
                else {
                    console.log(`[${peerId}] Non-recvonly connection failed. Closing peer connection.`);
                    pendingCandRef.current[peerId] = []
                }

            }
            else if (pc.connectionState === 'connected') {
                console.log(`[${peerId}] Connection established successfully.changeCount:${changeCount}`);
                if (changeCount === 0) {
                    // changeStream();
                    changeCount++;
                }
                // pc.getStats().then(stats => {
                //     stats.forEach(report => {
                //         console.log(`[${peerId}] Stats Report:`, report);
                //     });
                // });
            }
        };

        pc.ontrack = event => {
            // setUsers(prevUsers => [...prevUsers, { id: peerId, socket: socket, stream: event.streams[0] }]);
            // setUsers 처리.
            // 1.prev.some(u => u.id === peerId) → 이미 같은 id가 있으면 true
            // 2.true면 map으로 순회하면서 해당 id의 특성 업데이트 및 추가 객체를 반환
            // 3.false면 기존 배열에 새 객체 추가
            // ...user : user의 나머지 속성들을 복사

            const stream = event.streams[0];
            const socket = socketRef.current;
            if (socket) {
                setUsers(prev =>
                    prev.some(user => user.id === peerId)
                        ? prev.map(user => user.id === peerId ? { ...user, stream: event.streams[0] } : user)
                        : [...prev, { id: peerId, socket: socket, stream: event.streams[0] }]
                );

                // Scalable K-트리 구조에서 root broadcaster는 remoteStream을 로컬 스트림으로 설정
                localStreamRef.current = stream; // 받은 스트림을 로컬 스트림으로 설정
                // 보낼 스트림을 최상단 화면에 띄우기 위함
                if (localVideoRef.current) {
                    localVideoRef.current.srcObject = localStreamRef.current;
                }
            }
            console.log(`[Peer] Received remote stream  from ${peerId}`, stream?.getVideoTracks());
            // console.log(`[Peer] RTCPeerConnection getStats`, pc.getStats());
            if (hasStreamChangedRef.current) {
                console.log(`[Peer] Stream has been changed before for ${peerId}, replacing tracks...`);
                const newVideo = stream.getVideoTracks()[0] ?? null;
                const newAudio = stream.getAudioTracks()[0] ?? null;

                const pcs = pcsRef.current;
                if (!pcs) return;

                Object.entries(pcs).forEach(([remotePeerId, childPc]) => {
                    // '자식/다른 연결'에만 보내고 싶으면 부모는 제외
                    if (childPc === pc) return;

                    childPc.getSenders().forEach((sender) => {
                        if (!sender.track) return;

                        if (sender.track.kind === "video") {
                            sender.replaceTrack(newVideo);
                        }
                        if (sender.track.kind === "audio") {
                            sender.replaceTrack(newAudio);
                        }
                    });

                    console.log(`[P4] replaceTrack() to ${remotePeerId}: video=${!!newVideo}, audio=${!!newAudio}`);
                });
            }
            if (!hasStreamChangedRef.current) hasStreamChangedRef.current = true;
        };

        pc.onicegatheringstatechange = () => {
            console.log(`[Peer] ICE gathering state changed: ${pc.iceGatheringState}`);
            if (pc.iceGatheringState === 'complete') {
                console.log(`[Peer] ICE gathering complete for ${peerId}. Total candidates gathered: ${iceCandidateGatheredArrayRef.current[peerId]?.length}`);
                // sendIceCandidate(peerId);
            }
        }


        return pc;
    }, [socketRef.current, myid]); // 의존성 배열이 비어있으므로 이 함수는 컴포넌트가 처음 렌더링될 때 한 번만 생성

    // ICE send
    const sendIceCandidate = useCallback((peerId: string) => {
        const candArray = iceCandidateGatheredArrayRef.current[peerId] || [];
        const pc = pcsRef.current[peerId];
        console.log(`[Peer] ICE candidate gathering state before sent: ${pc.iceGatheringState}`);
        console.log(`[Peer] Sending ICE candidates to signaling server... count:${candArray.length}`, " ", candArray);
        if (candArray.length === 0) {
            console.log(`[Peer] No ICE candidates to send.`);
            return;
        }
        else {
            socketRef.current?.emit('candidateArray', { to: peerId, data: candArray });
            console.log(`[Peer] Sent ${candArray.length} ICE candidates to ${peerId}`);
        }
        // iceCandidateGatheredArrayRef.current[peerId].splice(0, iceCandidateGatheredArrayRef.current[peerId].length); // 배열 초기화

    }, []);
    // callback 함수로 정의 0906추가    
    // const softReset = useCallback(async (peerid: string) => {
    //     const pc = pcsRef.current[peerid];
    //     if (!pc) {
    //         console.error('RTCPeerConnection is not initialized.');
    //         return;
    //     }
    //     // 추후에 로직 변경 필요
    //     console.log(`[Peer] Soft Resetting: myid:${myidRef.current} peerid:${peerid}`);
    //     if (myidRef.current >= peerid) {
    //         console.log(`[Peer] I'm the offerer`);
    //         // ICE candidate 재설정 및 재협상
    //         // Network changed, ICE failed, Opponent requests 등  
    //         const offer = await pc.createOffer({ iceRestart: true });
    //         await pc.setLocalDescription(offer);
    //         socket.emit('signal', { to: peerid, data: offer });
    //         console.log(`[Peer] Sent Offer with ICE restart`);
    //     }

    // }, [socket, myid]);

    // const hardReset = useCallback(async (peerid: string) => {
    //     console.log(`[Peer] Hard Resetting function called for peerid:${peerid}`);
    //     // 1. 기존 피어 연결 종료
    //     const pc = pcsRef.current[peerid];
    //     if (!pc) {
    //         console.error('[Peer][RESET] RTCPeerConnection is not initialized.');
    //         // return;
    //     }
    //     else {
    //         console.log(`[Peer][RESET] Hard Resetting: myid:${myidRef.current} peerid:${peerid}`);
    //         pc.close();
    //         delete pcsRef.current[peerid];
    //         delete pcTypesRef.current[peerid];
    //         delete pendingCandRef.current[peerid];
    //         // 2. setUsers에서 해당 유저 제거
    //         setUsers(prev => prev.filter(user => user.id !== peerid));
    //     }

    //     console.log(`[Peer][RESET] Closed old connection and created new one for ${peerid}`);

    //     // 3. 새로운 피어 연결 생성
    //     // const newPc = createPeerConnection(peerid, 'recvonly');
    //     // pcsRef.current[peerid] = newPc;

    //     // // 4. 새로운 연결에 대해 offer/answer 교환
    //     // console.log(`[Peer][RESET] I'm the offerer`);
    //     // const offer = await newPc.createOffer();
    //     // await newPc.setLocalDescription(offer);
    //     // socketRef.current?.emit('offer', { to: peerid, data: offer });
    //     // console.log(`[Peer][RESET] Sent Offer`);

    //     // 5. 방 재접속 요청; 로직 추가 중 2025-12-24 
    //     /* 서버쪽에서 activateBroadcasters에서 확인한 후 확인이 되면, 전의 topology를 지우자 
    //         현재 아이디는 그대로 받음 ; myid도 초기화 시키고 재요청하도록 코드 수정
    //     */
    //     socketRef.current?.emit('join', { room: room, type: 'broadcast', myid: myidRef.current });
    // }, []);

    // expose hardReset via ref so handlers created earlier can call it safely
    // hardResetRef.current = hardReset;

    const renegotiateSamePc = useCallback(async (peerId: string) => {
        const pc = pcsRef.current[peerId];
        const socket = socketRef.current;
        if (!pc || !socket) return;
        try {

            // signalingState가 안정적일 때만 시도하는 게 안전함
            if (pc.signalingState !== "stable") {
                console.log(`[${peerId}] signalingState=${pc.signalingState}, waiting for stable.`);
                // 필요하면 여기서 일정 시간 후 재시도하도록 해도 됨
                return;
            }

            console.log(`[${peerId}] Recreating offer (iceRestart=true) on same pc...`);

            // 핵심: 같은 pc에서 ICE restart + offer 재생성
            const offer = await pc.createOffer({ iceRestart: true });

            // 너가 쓰던 SDP bandwidth 제한 로직 유지 가능
            const newSdp = setMaxBandwidth(offer.sdp || '', 'video', 512000);
            const localDesc = newSdp ? { type: offer.type, sdp: newSdp } : offer;

            await pc.setLocalDescription(localDesc);

            // 서버로 offer 전송 (기존 이벤트명 유지)
            socket.emit('offer', { to: peerId, data: localDesc });
            console.log(`[${peerId}] Renegotiation offer sent.`);
        } catch (e) {
            console.error(`[${peerId}] renegotiation failed`, e);
        }
    }, []);


    const forceDisconnectPeer = (peerId: string) => {
        const pc = pcsRef.current[peerId];
        if (!pc) {
            console.warn(`[forceDisconnectPeer] no pc for peerId=${peerId}`);
            return false;
        }

        try {
            // 이벤트 핸들러 제거 (중복 cleanup/메모리 누수 방지)
            // pc.onicecandidate = null;
            // pc.ontrack = null;
            // pc.onconnectionstatechange = null;
            // pc.oniceconnectionstatechange = null;
            // pc.onsignalingstatechange = null;

            // 연결 강제 종료
            pc.close();
        } catch (e) {
            console.error(`[forceDisconnectPeer] error closing pc for ${peerId}`, e);
        }

        // 레퍼런스/상태 정리
        // delete pcsRef.current[peerId];
        // delete pcTypesRef.current[peerId];
        // if (pendingCandRef.current) pendingCandRef.current[peerId] = [];

        // setUsers(prev => prev.filter(u => u.id !== peerId));

        console.log(`[forceDisconnectPeer] disconnected peerId=${peerId}`);
        return true;
    };
    forceDisconnectPeerRef.current = forceDisconnectPeer;

    useEffect(() => {
        // 디버그용 전역 노출
        (window as any).forceDisconnectPeer = (peerId: string) => {
            return forceDisconnectPeerRef.current(peerId);
        };

        // (선택) 현재 pcsRef도 보고 싶으면 같이 노출
        (window as any).pcsRef = pcsRef;

        return () => {
            delete (window as any).forceDisconnectPeer;
            delete (window as any).pcsRef;
        };
    }, []);


    return (
        <div style={{ padding: 16, fontFamily: "system-ui, sans-serif" }}>
            <h2>WebRTC Peer (React)</h2>
            <h3>내가 전달할 Stream</h3>
            <div style={{ display: 'flex', width: 480, height: 240 }}>
                <video
                    ref={localVideoRef}
                    autoPlay
                    playsInline
                    muted
                    style={{ width: '100%', height: '100%', background: "#000" }}
                />

                {/* 2. 비디오 위에 표시할 라벨 */}
                <div
                    style={{
                        position: 'absolute', // 부모 div를 기준으로 위치를 정함
                        top: '10px',          // 위에서 10px 떨어짐
                        left: '10px',         // 왼쪽에서 10px 떨어짐
                        color: 'white',       // 글자색
                        backgroundColor: 'rgba(0, 0, 0, 0.5)', // 반투명 배경
                        padding: '5px 10px',  // 안쪽 여백
                        borderRadius: '5px',  // 모서리 둥글게
                        fontSize: '14px'
                    }}
                >
                    {myid}
                </div>
                <button onClick={() => (changeStream())}>Change Stream</button>
            </div>




            {
                users.map((user) => (
                    <div key={user.id}>
                        <Video peerId={user.id} stream={user.stream} />
                        <div style={{ marginTop: '5px' }}>
                            {/* <button onClick={() => setVideoBitrate(user.id, BitrateConfig.min)}>Min</button> */}
                            {/* <button onClick={() => setVideoBitrate(user.id, BitrateConfig.medium)}>Medium</button> */}
                            {/* <button onClick={() => setVideoBitrate(user.id, BitrateConfig.max)}>Max</button> */}
                            {<button onClick={() => sendIceCandidate(user.id)}>Send ICE</button>}

                        </div>
                    </div>
                ))}

            <div style={{ marginTop: 16 }}>

                <pre style={{ background: "#f6f6f6", padding: 12, maxHeight: 240, overflow: "auto" }}>
                </pre>
            </div>
        </div>

    );
}





export default App;