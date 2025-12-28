/*
    WebRTC Peer (React + TypeScript)
    1. Candidate 개별 전송
    2. Candidate 개별 수신 및 처리
    ++ 연결 실패시 Candidate 배열로 송수신 받아 Loop문으로 addIceCandidate 처리
    3. 화면 공유 스트림 교체 기능
    4. 비트레이트 설정 기능
    5. 1_TO_N / MESH 모드 선택 기능

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
    min: 50000,   // 50 kbps
    medium: 1000000, // 1 Mbps
    max: 2000000,  // 2 Mbps
};

const BITRATE: number = 500; // 500 kbps. setMaxBandwidth를 이용하는 경우에만 이 값을 적용해야 함. (setVideoBitrate는 기본 단위가 kbps가 아니라 bps임.) 

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
const MODE: string = 'MESH'; // '1_TO_N' or 'MESH'

// 소켓 인스턴스를 컴포넌트 외부에서 한 번만 생성하여 재렌더링 시 재생성을 방지?
export const SIGNALING_SERVER_URL = `https://192.168.0.6:8000`

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

const config = {};
// const pcConfig : RTCConfiguration = {"iceServers":[]};

function App() {
    console.log('Rendering... ');
    let changeCount = 0;
    const room = 'testRoom'; // Example room name

    // Record<<K,T> : TS utility type

    /* UseRef 사용하지 않으면 랜더링시 초기화 문제 발생 */
    const socketRef = useRef<SocketIOClient.Socket | null>(null);
    const pcsRef = useRef<Record<string, RTCPeerConnection>>({}); // peerId를 키(Key)로 하여 여러 명과의 연결 객체를 관리하고 있음

    /* Others candidates against me */
    const pendingCandRef = useRef<Record<string, RTCIceCandidate[]>>({});

    /* My candidates against others */
    const iceCandidateGatheredArrayRef = useRef<Record<string, RTCIceCandidate[]>>({});

    // const pcRef = useRef<RTCPeerConnection>(null);
    const localStreamRef = useRef<MediaStream>(null);
    const localVideoRef = useRef<HTMLVideoElement>(null);
    const myidRef = useRef<string>('');
    const localStreamSortRef = useRef<string>('userMedia');
    // Offerer / Answerer 구분 저장
    const pcTypesRef = useRef<Record<string, string>>({});
    // pc Close Test 용 버튼 
    const forceDisconnectPeerRef = useRef<(peerId: string) => boolean>(() => false);

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
                console.log(`[Peer] Video bitrate for ${peerId} set to ${bitrate / 1000}kbps.`);
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
     * @param maxKbps 설정할 최대 대역폭 값 (Kilobits per second).
     * @returns 수정된 SDP 문자열.
     */

    function setMaxBandwidth(sdp: string, mediaType: string, maxKbps: number): string {
        console.log(`[SDP] Setting max bandwidth for ${mediaType} to ${maxKbps} kbps`);
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
                // b=AS:{maxKbps} 라인을 m= 바로 뒤에 추가
                if (insideTargetMediaSection) {
                    // RFC 4566에서 'AS' 타입은 애플리케이션 특정 최대 대역폭을 의미
                    newSdp.push(`b=AS:${maxKbps}`);

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

            // localStreamRef.current = await navigator.mediaDevices.getDisplayMedia({ // 화면 공유
            //     video: {

            //         width: { ideal: 480, max: 480 },
            //         height: { ideal: 320, max: 320 },
            //         frameRate: { ideal: 30, max: 30 },
            //     },
            //     audio: true
            // });

            localStreamRef.current = (await navigator.mediaDevices.getUserMedia({ // 카메라
                video: {

                    width: { ideal: 720, max: 1920 },
                    height: { ideal: 480, max: 1080 },
                    frameRate: { ideal: 30, max: 60 },
                },
                audio: true
            }));

            if (localVideoRef.current) {
                localVideoRef.current.srcObject = localStreamRef.current;
            }


            console.log('[Peer] Connecting to signaling server...  ');
            // useEffect로 이동하면, localStream과 Sync 문제 발생
            // 수동으로 연결 시작
            socketRef.current?.connect(); // 스트림 획득 후 소켓 연결
        }
        catch (error) {
            console.error('Error accessing media devices.', error);
        }
    }, []);

    /* 스트림 교체 함수 */
    const changeStream = useCallback(async () => {

        if (localStreamSortRef.current === 'userMedia') {
            console.log(`[Peer] Current stream is not a display source. Changing stream...`);
            localStreamRef.current = await navigator.mediaDevices.getDisplayMedia({
                video: {
                    width: { ideal: 854, max: 1280 },
                    height: { ideal: 480, max: 720 },
                    frameRate: { ideal: 15, max: 30 },
                },
                audio: true
            });

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
                        sender.replaceTrack(track); // 세션을 유지하면서 트랙을 교체
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
        console.log('UserMode:', MODE);

        getLocalStream(); // 미디어 획득하고 시그널링과 연결 시도
        console.log('Local stream obtained:', localStreamRef.current);



        socketRef.current.on('connect', () => { // connect 이벤트 수신 시 
            console.log('[Peer] Connected to signaling server');
            if (MODE === '1_TO_N') {
                console.log('[Peer] Joining room in 1_TO_N mode:', room);
                socketRef.current?.emit('join', { room, type: 'mesh' });
            }
            else if (MODE === 'MESH') {
                console.log('[Peer] Joining room in MESH mode:', room);
                socketRef.current?.emit('join', { room, type: '1_to_n' });
            }
        });


        socketRef.current.on('my-id', (id: string) => { // 아이디 수신하고 세팅
            console.log('[Peer] My ID:', id);
            myidRef.current = id;
            setMyid(id);
            console.log('My ID set to state:', myidRef.current);
        });


        socketRef.current.on('existing-peers', async (peers: string[]) => { // 이미 방에 있던 사용자들의 목록(existing-peers) 수신 (새로 참여 시)
            console.log('[Peer] Existing peers in room:', peers);

            // Staggered Connection: 순차적으로 연결하여 Signaling Storm 방지
            for (const peerid of peers) {
                console.log('[Peer] createPeerConnection:', peerid);
                const pc = createPeerConnection(peerid, 'both');
                // Store the peer connection in the ref
                pcsRef.current[peerid] = pc;

                // 보내기 전 Bit rate 설정
                // setVideoBitrate(peerid, BitrateConfig.min)

                const offer = await pc.createOffer();
                const newSdp = setMaxBandwidth(offer.sdp || '', 'video', BITRATE); // if BITRATE = 51200 = 비디오 대역폭을 51.2Mbps로 설정
                await pc.setLocalDescription(newSdp ? { type: offer.type, sdp: newSdp } : offer);
                socketRef.current?.emit('offer', { to: peerid, data: newSdp ? { type: offer.type, sdp: newSdp } : offer });
                // peerid에 대해서 내가 offer 보냄 -> Offerer 기록
                pcTypesRef.current[peerid] = 'offerer';
                console.log(`[Peer] Sent Offer to ${peerid}`, newSdp ? { type: offer.type, sdp: newSdp } : offer);

                // 20명 연결 시 부하 분산을 위해 100ms 지연
                await new Promise(resolve => setTimeout(resolve, 10));
            }
        });


        socketRef.current.on('offer', async ({ from, data }: { from: string, data: any }) => { // Offer 수신 및 Answer 전송 (기존 참여자)
            console.log(`[Peer] Received offer from ${from}`, data);

            pcsRef.current[from] = createPeerConnection(from, 'both'); // RTCPeerConnection 생성
            const pc = pcsRef.current[from];
            // 보내기 전 Bit rate 설정
            // setVideoBitrate(from, BitrateConfig.min)

            await pc.setRemoteDescription(new RTCSessionDescription(data));

            // RemoteDescription 설정 직후 대기열 처리!! <추가!!>
            await flushPendingCandidates(from);

            const answer = await pc.createAnswer(); // answer 생성

            const newSdp = setMaxBandwidth(answer.sdp || '', 'video', BITRATE); // 비디오 대역폭 설정. if BITRATE = 10000 = 10Mbps
            await pc.setLocalDescription(newSdp ? { type: answer.type, sdp: newSdp } : answer);
            // await pc.setLocalDescription(answer);
            // socketRef.current?.emit('answer', { to: from, data: answer });
            socketRef.current?.emit('answer', { to: from, data: newSdp ? { type: answer.type, sdp: newSdp } : answer });
            // peerid에 대해서 내가 answer 보냄 -> Answerer 기록
            pcTypesRef.current[from] = 'answerer';
            console.log(`[Peer] Sent Answer to ${from}`, newSdp ? { type: answer.type, sdp: newSdp } : answer);
        });

        socketRef.current.on('answer', async ({ from, data }: { from: string, data: any }) => { // Answer 수신 및 ICE 후보 전송 (새로운 참여자)
            const pc = pcsRef.current[from];
            if (!pc) {
                console.error('RTCPeerConnection is not initialized.');
                return;
            }
            console.log(`[Peer] Received answer.${from}`, data);
            await pc.setRemoteDescription(new RTCSessionDescription(data));

            // RemoteDescription 설정 직후 대기열 처리!! <추가!!>
            await flushPendingCandidates(from);
        });

        // 배열 수신 이벤트
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

        // 수신 측. 개별 수신
        /*
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
        */

        // 수정된 candidate 개별 수신 이벤트
        socketRef.current.on('candidate', async ({ from, data }: { from: string, data: any }) => {
            const pc = pcsRef.current[from];
            if (!pc || !data.candidate) return;

            // RemoteDescription이 이미 설정되어 있다면 -> 즉시 추가
            if (pc.remoteDescription) {
                try {
                    await pc.addIceCandidate(new RTCIceCandidate(data.candidate));
                    console.log(`[Peer] Added ICE candidate from ${from}`);
                } catch (e) {
                    console.warn(`[Peer] Failed to add ICE candidate from ${from}`, e);
                }
            }
            // RemoteDescription이 없다면 -> 배열에 보관
            else {
                if (!pendingCandRef.current[from]) {
                    pendingCandRef.current[from] = [];
                }
                pendingCandRef.current[from].push(data.candidate);
                console.log(`[Peer] RemoteDescription not ready. Buffered candidate from ${from}. Total: ${pendingCandRef.current[from].length}`);
            }
        });

        socketRef.current.on('disconnected', (peerId: string) => {
            console.log(`[Peer] Peer ${peerId} disconnected.`);

        });
        /*
        return () => {
            if (socketRef.current) {
                socketRef.current.disconnect();
            }
            Object.keys(pcsRef.current).forEach((key) => {
                pcsRef.current[key].close();
                delete pcsRef.current[key];
            });
        };
        */

        // Cleanup 로직 수정
        return () => {
            console.log('[App] Cleaning up resources...');

            // 소켓 연결 종료
            if (socketRef.current) {
                socketRef.current.disconnect();
                // socketRef.current = null; // 필요 시 제거
            }

            // 카메라/마이크 하드웨어 장치 끄기
            if (localStreamRef.current) {
                localStreamRef.current.getTracks().forEach(track => {
                    track.stop(); // 하드웨어 리소스 반환
                });
                localStreamRef.current = null;
            }

            // 모든 PeerConnection 종료 및 이벤트 리스너 제거
            if (pcsRef.current) {
                Object.keys(pcsRef.current).forEach((key) => {
                    const pc = pcsRef.current[key];
                    if (pc) {
                        // 이벤트 핸들러 해제 (GC 도움)
                        pc.onicecandidate = null;
                        pc.ontrack = null;
                        pc.oniceconnectionstatechange = null;

                        // 연결 종료
                        pc.close();
                        console.log(`[App] Closed connection with ${key}`);
                    }
                    delete pcsRef.current[key];
                });
            }
        };
    }, []);

    // useCallback을 사용하여 createPeerConnection 함수를 메모이제이션
    // peerId - parameter, RTCPeerConnection - return type

    // 새로운 피어와 연결될 때마다 호출되어 RTCPeerConnection 객체를 생성
    const createPeerConnection = useCallback((peerId: string, type: string): RTCPeerConnection => {

        console.log(`[Peer] createPeerConnection ${peerId}`);
        const pc = new RTCPeerConnection(pcConfig);
        // const pc = new RTCPeerConnection(config);

        // 로컬 트랙 추가
        if (type === 'recvonly') {
            console.log(`[Peer] Setting up recvonly connection `);
            pc.addTransceiver('video', { direction: 'recvonly' });
            pc.addTransceiver('audio', { direction: 'recvonly' });
        }
        else {

            if (localStreamRef.current !== null) {
                console.log('[Peer] Add local stream to peer connection');
                localStreamRef.current.getTracks().forEach(track => {
                    pc.addTrack(track, localStreamRef.current!);
                });

            } else {
                console.error('Local media stream is null');
            }

        }

        // ICE Candidate 수집 핸들러... 송신 측
        pc.onicecandidate = event => {
            // console.log('[Peer] ICE candidate event:', event);
            const socket = socketRef.current;

            if (iceCandidateGatheredArrayRef.current[peerId] === undefined) {
                iceCandidateGatheredArrayRef.current[peerId] = []; // 수집된 Candidate를 저장할 배열?
            }

            if (event.candidate && socket != null) {
                // 수집된 Candidate를 시그널링 서버로 전송
                socket.emit('candidate', { to: peerId, data: { type: 'candidate', candidate: event.candidate } });

                iceCandidateGatheredArrayRef.current[peerId].push(event.candidate); // 수집된 Candidate를 배열에 저장

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

        // 연결 상태 모니터링
        pc.onconnectionstatechange = async () => {
            console.log(`[${peerId}]${pc.connectionState} state:`, pc.connectionState);

            if (pc.connectionState === 'disconnected' || pc.connectionState === 'closed') {
                console.log(`[${peerId}] Connection . ${pcTypesRef.current[peerId]}Attempting renegotiate.`);
                // 재연결 시도 join-redial for offerer connections
                const pcType = pcTypesRef.current[peerId];
                // Offerer 인지 확인 후 renegotiate 
                if (pcType === 'offerer') {
                    renegotiateSamePc(peerId);
                }
            }
            else if (pc.connectionState === 'failed') {
                // 재연결 시도 Redial
                // console.log(`[${peerId}] Connection failed. Attempting to restart ICE...`);
                const pcType = pcTypesRef.current[peerId];
                if (pcType === 'offerer') {
                    try {
                        const targetPc = pcsRef.current[peerId];
                        console.log(`[${peerId}] offerer connection lost. Attempting redial.`);
                        if (targetPc) {
                            targetPc.close();
                            delete pcsRef.current[peerId];
                            delete pcTypesRef.current[peerId];
                            pendingCandRef.current[peerId] = [];
                            iceCandidateGatheredArrayRef.current[peerId] = [];
                            setUsers(prev => prev.filter(u => u.id !== peerId));
                        }
                        else {
                            console.log(`[${peerId}] No existing peer connection found for hard reset.`);
                        }
                        console.log(`[${peerId}] Redialing...`);
                        socketRef.current?.emit('join', { room: room, type: 'redial', to: peerId });
                    }
                    catch (e) {
                        console.error(e);
                    }
                }
                else {
                    console.log(`[${peerId}] Non-offerer connection failed. Closing peer connection.`);
                    
                }

            }

            else if (pc.connectionState === 'connected') {
                console.log(`[${peerId}] Connection established successfully.changeCount:${changeCount}`);
                if (changeCount === 0) {
                    // changeStream(); // 스트림 교체 함수 호출 (카메라 -> 화면공유)
                    changeCount++;
                }
                // pc.getStats().then(stats => {
                //     stats.forEach(report => {
                //         console.log(`[${peerId}] Stats Report:`, report);
                //     });
                // });
            }
        };

        // 수신 측. 트랙 수신 핸들러. P2P 연결이 성공하고 미디어 데이터가 넘어오기 시작하면 ontrack 이벤트가 발생
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
            }
            console.log(`[Peer] Received remote stream  from ${peerId}`, stream?.getVideoTracks());
            console.log(`[Peer] RTCPeerConnection getStats`, pc.getStats());


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
            // 모아두었던 candidate들을 한 번에 전송
            socketRef.current?.emit('candidateArray', { to: peerId, data: candArray });
            console.log(`[Peer] Sent ${candArray.length} ICE candidates to ${peerId}`);
        }
        // iceCandidateGatheredArrayRef.current[peerId].splice(0, iceCandidateGatheredArrayRef.current[peerId].length); // 배열 초기화

    }, []);

    // 대기 중인 Candidate들을 일괄 처리하는 함수
    const flushPendingCandidates = async (peerId: string) => {
        const pc = pcsRef.current[peerId];
        const pendingCandidates = pendingCandRef.current[peerId];

        if (pc && pendingCandidates && pendingCandidates.length > 0) {
            console.log(`[Peer] Flushing ${pendingCandidates.length} candidates for ${peerId}`);
            for (const candidate of pendingCandidates) {
                try {
                    await pc.addIceCandidate(new RTCIceCandidate(candidate));
                } catch (e) {
                    console.warn(`[Peer] Failed to add buffered candidate`, e);
                }
            }
            // 처리 후 배열 초기화
            pendingCandRef.current[peerId] = [];
        }
    };

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

            <div style={{ display: 'flex', width: 480, height: 240 }}>
                <video
                    ref={localVideoRef}
                    autoPlay
                    playsInline
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