/*
    WebRTC Peer (React + TypeScript)
    1) 로컬 미디어 획득 → 시그널링 서버 연결
    2) 방 참가(join) → 기존 피어 목록 수신(existing-peers)
    3) Offer/Answer 교환 → RemoteDescription 설정
    4) ICE Candidate 개별 전송/수신 + RemoteDescription 전 후보 버퍼링
    5) RemoteDescription 설정 직후 후보 큐 flush 처리
    6) 연결 상태 모니터링 및 실패 시 redial/정리 처리
    7) 스트림 교체(화면공유) 시 replaceTrack 기반 트랙 교체
    8) 비트레이트 제한(SDP b=AS 삽입 / sender.setParameters)

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

const BITRATE: number = 50000; // <DG> 50Mbps. setMaxBandwidth를 이용하는 경우에만 이 값을 적용해야 함. (setVideoBitrate는 기본 단위가 kbps가 아니라 bps임.) 

const MAX_REDIAL_ATTEMPTS = 2; // 최대 재연결 시도 횟수

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

const constraints = { // <DG> 해상도 및 프레임레이트 제약 설정 프리셋
    video: {
        width: { ideal: 1280, max: 1920 }, // max를 1920으로 수정
        height: { ideal: 720, max: 1080 }, // max를 720에서 1080으로 수정
        frameRate: { ideal: 60, max: 60 },
    },
    audio: true
};

// 운영 모드 설정
// const MODE: string = '1_TO_N'; // '1_TO_N' or 'MESH'

// 소켓 인스턴스를 컴포넌트 외부에서 한 번만 생성하여 재렌더링 시 재생성을 방지?
export const SIGNALING_SERVER_URL = `https://192.168.1.4:8000`

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
                // 'stun:23.21.150.121:3478',

            ]
        },
    ],
};

const config = {};
// const pcConfig : RTCConfiguration = {"iceServers":[]};

// const pcConfig : RTCConfiguration = {"iceServers":[]};
function App() {
    console.log('Rendering... ');
    // URL 파라미터에서 codec 값을 읽어와 테스트용 코덱으로 설정 (기본값 H264)
    // 예 ) https://192.168.1.4:3000/?codec=H264
    const codecParam = new URLSearchParams(window.location.search).get('codec');
    const targetMimeType = codecParam ? `video/${codecParam.toUpperCase()}` : 'video/H264';
    let changeCount = 0;
    const room = 'testRoom'; // Example room name

    // Record<<K,T> : TS utility type

    /* UseRef 사용하지 않으면 랜더링시 초기화 문제 발생 */
    /* useRef 기반 상태 보존: 렌더링과 무관한 연결 객체/버퍼/스트림 보존 */
    const socketRef = useRef<SocketIOClient.Socket | null>(null);
    /* 피어별 RTCPeerConnection 관리: peerId → RTCPeerConnection 매핑 */
    const pcsRef = useRef<Record<string, RTCPeerConnection>>({}); // peerId를 키(Key)로 하여 여러 명과의 연결 객체를 관리하고 있음
    /* 피어별 Redial 횟수관리: peerId → Redial 매핑 */
    const redialCountsRef = useRef<Record<string, number>>({});
    /* 상대 ICE 후보 버퍼링: RemoteDescription 미설정 시 후보 임시 저장 */
    const pendingCandRef = useRef<Record<string, RTCIceCandidate[]>>({});
    /* 로컬 ICE 후보 수집 배열: peerId별 ICE 후보 모아두기 */
    const iceCandidateGatheredArrayRef = useRef<Record<string, RTCIceCandidate[]>>({});
    /* 상대 ICE 후보 버퍼링: RemoteDescription 미설정 시 후보 임시 저장 */
    const localStreamRef = useRef<MediaStream>(null);
    const localVideoRef = useRef<HTMLVideoElement>(null);

    const myidRef = useRef<string>('');
    const localStreamSortRef = useRef<string>('userMedia');

    const hasStreamChangedRef = useRef<boolean>(false);
    const pcTypesRef = useRef<Record<string, string>>({});

    // 사용자 상태(React state)
    const [users, setUsers] = useState<WebRTCUser[]>([]);
    const [myid, setMyid] = useState<string>('');
    const [notice, setNotice] = useState<string>('');
    const noticeTimerRef = useRef<number | null>(null);
    const isRootRef = useRef<boolean>(false);


    // pc Close Test 용 버튼 
    const forceDisconnectPeerRef = useRef<(peerId: string) => boolean>(() => false);
    /* 송신 비트레이트 설정: RTCRtpSender.setParameters 기반 */
    const TIMEOUT_DURATION = 0; //0초

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

                // <DG> 해상도 우선 설정 추가 2026.01.29.
                //parameters.degradationPreference = 'maintain-resolution';

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

            //localStreamRef.current = await navigator.mediaDevices.getUserMedia(constraints);

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
        // 수동으로 연결 시작
        socketRef.current?.connect();
        console.log('Local stream obtained:', localStreamRef.current);


        socketRef.current.on('connect', () => {
            console.log('[Peer] Connected to signaling server');
            socketRef.current?.emit('join', { room: room, type: 'broadcast' });
        });
        socketRef.current.on('root-broadcaster', () => {
            isRootRef.current = true; // [추가] 내가 원본 방송자임을 마킹!
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
            // <DG> 중복된 연결 생성 방지 로직 추가
            if (pcsRef.current[parentid]) {
                console.warn(`[Peer] Connection to ${parentid} already exists. Skipping duplicate existing-peers event.`);
                return; // for문 건너뛰어 다음 peerid로 이동하여 중복된 연결 생성 방지
            }

            // 트리구조이기 때문에 recvonly 연결 생성
            const pc = createPeerConnection(parentid, 'recvonly');
            // Store the peer connection in the ref
            pcsRef.current[parentid] = pc;

            // 보내기 전 Bit rate 설정
            // setVideoBitrate(peerid, BitrateConfig.min)

            const offer = await pc.createOffer();
            /*const newSdp = setMaxBandwidth(offer.sdp || '', 'video', BITRATE);*/
            //await pc.setLocalDescription(newSdp ? { type: offer.type, sdp: newSdp } : offer);
            await pc.setLocalDescription(offer); // 조작없는 순정 offer를 세팅
            //socketRef.current?.emit('offer', { to: parentid, data: newSdp ? { type: offer.type, sdp: newSdp } : offer });
            socketRef.current?.emit('offer', { to: parentid, data: offer });
        });


        socketRef.current.on('offer', async ({ from, data }: { from: string, data: any }) => {
            console.log(`[Peer] Received offer from ${from}`, data);

            // 트리구조이기 때문에 sendonly 연결 생성
            pcsRef.current[from] = createPeerConnection(from, 'sendonly');
            const pc = pcsRef.current[from];
            // 보내기 전 Bit rate 설정
            // setVideoBitrate(from, BitrateConfig.min)

            await pc.setRemoteDescription(new RTCSessionDescription(data));
            // RemoteDescription 설정 직후 대기열 처리!! <DG>
            await flushPendingCandidates(from);
            const answer = await pc.createAnswer();

            let finalSdp: any = answer;
            if (isRootRef.current) {
                // 내가 방장이면 높은 비트레이트를 SDP에 강제로 삽입
                const newSdp = setMaxBandwidth(answer.sdp || '', 'video', BITRATE);
                finalSdp = newSdp ? { type: answer.type, sdp: newSdp } : answer;
            }

            await pc.setLocalDescription(finalSdp);
            socketRef.current?.emit('answer', { to: from, data: finalSdp });
            // console.log(`[Peer] Sent Answer to ${from}`, finalSdp);
        });

        socketRef.current.on('answer', async ({ from, data }: { from: string, data: any }) => {
            const pc = pcsRef.current[from];
            if (!pc) {
                console.error('RTCPeerConnection is not initialized.');
                return;
            }
            console.log(`[Peer] Received answer.${from}`, data);
            await pc.setRemoteDescription(new RTCSessionDescription(data));

            // RemoteDescription 설정 직후 대기열 처리!! <DG>
            await flushPendingCandidates(from);
        });
        // 배열 수신 ; 현재 안씀
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
                }
                else if (pc.signalingState === 'closed') {
                    console.warn(`[Peer] Ignored ICE candidateArray from ${from} because PC is closed.`)
                    return;
                }
                else {
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


                }

            }
        });
        // 개별 수신
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

        /* 선생님이 퇴장했기때문에 서버로부터 강제 종료 메시지 수신*/
        socketRef.current.on('force-disconnect-room', () => {
            console.log(`[Peer] Force disconnect by server for all peers.`);
            // Show a brief notice to the user for 3 seconds
            if (noticeTimerRef.current) {
                // 이전에 설정된 타이머를 취소 -> 에러 방지
                clearTimeout(noticeTimerRef.current);
                noticeTimerRef.current = null;
            }
            setNotice('선생님이 퇴장하였습니다 !');
            // setTimeout 구문 안의 동작이 설정된 시간(3초) 후에 실행되도록 타이머 설정
            noticeTimerRef.current = window.setTimeout(() => {
                setNotice('');
                noticeTimerRef.current = null;
            }, 3000);
            // 서버와 연결된 socket은 그대로 모든 피어 연결(PC) 종료
            Object.keys(pcsRef.current).forEach((key) => {
                // 연결 강제 종료
                const targetPc = pcsRef.current[key];

                // PC cleanup 공통 로직
                if (targetPc) {
                    targetPc.close();
                }
                // pcsRef 초기화
                pcsRef.current = {};
                pcTypesRef.current = {};
                pendingCandRef.current = {};
                iceCandidateGatheredArrayRef.current = {};
                redialCountsRef.current = {};
                // 사용자 목록 초기화
                setUsers([]);
                // setUsers(prev => prev.filter(u => u.id !== key));
                console.log(`[Peer] Closed connection with ${key}`);
            });

        });

        socketRef.current.on('droppedOffer-redial', () => {
            console.log(`[Peer] Redial request dropped by server.`);
            socketRef.current?.emit('join', { room: room, type: 'redial' });
        });

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
            // clear any pending notice timer
            if (noticeTimerRef.current) {
                clearTimeout(noticeTimerRef.current);
                noticeTimerRef.current = null;
            }
        };

    }, []);

    // useCallback을 사용하여 createPeerConnection 함수를 메모이제이션
    // peerId - parameter; 나와 연결될 피어, RTCPeerConnection - return type
    const createPeerConnection = useCallback((peerId: string, type: string): RTCPeerConnection => {


        console.log(`[Peer] createPeerConnection ${peerId}`);
        // record the type of this peer connection so handlers can decide actions later
        pcTypesRef.current[peerId] = type;
        const pc = new RTCPeerConnection(pcConfig);
        // const pc = new RTCPeerConnection(config);
        if (type === 'recvonly') {
            console.log(`[Peer] Setting up recvonly connection `);
            const videoTransceiver = pc.addTransceiver('video', { direction: 'recvonly' });
            pc.addTransceiver('audio', { direction: 'recvonly' });



            // <코덱 설정>
            // [추가됨] 자식(수신처)이 Offer를 던질 때도 H.264를 1순위로 만들어야 함!
            if (videoTransceiver && 'setCodecPreferences' in videoTransceiver) {
                const capabilities = RTCRtpReceiver.getCapabilities('video');
                if (capabilities && capabilities.codecs) {
                    const h264Codecs = capabilities.codecs.filter(c => c.mimeType === 'video/H264');
                    if (h264Codecs.length > 0) {
                        try {
                            videoTransceiver.setCodecPreferences(h264Codecs);
                            console.log(`[Peer] H.264 preference set for recvonly connection`);
                        } catch (e) {
                            console.error('H264 preference failed for recvonly', e);
                        }
                    }
                }
            }


            // <코덱 설정>
            /*
            // [수정됨] 단일 코덱 강제(filter)에서 최우선 순위 지정(sort) 방식으로 변경
            if (videoTransceiver && 'setCodecPreferences' in videoTransceiver) {
                const capabilities = RTCRtpReceiver.getCapabilities('video');
                if (capabilities && capabilities.codecs) {

                    // 전체 코덱을 유지하되, 원하는 코덱을 리스트 맨 앞으로 끌어올림
                    const sortedCodecs = [...capabilities.codecs].sort((a, b) => {
                        const isTargetA = a.mimeType === targetMimeType;
                        const isTargetB = b.mimeType === targetMimeType;

                        if (isTargetA && !isTargetB) return -1; // a를 앞으로
                        if (!isTargetA && isTargetB) return 1;  // b를 앞으로
                        return 0; // 순서 유지
                    });

                    try {
                        // 정렬된 전체 코덱 리스트를 주입 (1순위 VP9, 실패 시 나머지 코덱 허용)
                        videoTransceiver.setCodecPreferences(sortedCodecs);
                        console.log(`[Peer] VP9 preference set at the top for recvonly connection`);
                    } catch (e) {
                        console.error('Codec preference failed for recvonly', e);
                    }
                }
            }
            */
        }
        else {
            // <DG> 기존 코드 주석 처리함. 2026.01.29.
            /*
            if (localStreamRef.current !== null) {
                console.log('[Peer] Add local stream to peer connection');
                localStreamRef.current.getTracks().forEach(track => {
                    // Scalable Mode: track에 다른 피어로부터 받은 Stream 
                    // (내 스트림이 아닌 전달할 Stream을 담으면 됨)
                    pc.addTrack(track, localStreamRef.current!);
                });
            }
            */

            if (localStreamRef.current !== null) {
                console.log('[Peer] Add local stream to peer connection');
                localStreamRef.current.getTracks().forEach(track => {
                    // pc.addTrack은 RTCRtpSender를 반환합니다.
                    const sender = pc.addTrack(track, localStreamRef.current!);

                    // 비디오 트랙인 경우 처리
                    if (track.kind === 'video') {
                        // [1] 원본 방장(Root)일 때만 해상도 절대 방어 옵션 부여 (릴레이 노드는 우회)
                        if (isRootRef.current) {
                            const parameters = sender.getParameters();
                            parameters.degradationPreference = 'maintain-resolution';
                            sender.setParameters(parameters)
                                .then(() => console.log(`[Peer] ${peerId} degradationPreference set to maintain-resolution`))
                                .catch(e => console.warn(`[Peer] Failed to set degradationPreference for ${peerId}`, e));
                        }

                        // <코덱 설정>

                        // [2] 하드웨어 가속기(HW Encoder)를 무조건 깨우도록 H.264 코덱 강제 적용
                        const transceiver = pc.getTransceivers().find(t => t.sender === sender);
                        if (transceiver && 'setCodecPreferences' in transceiver) {
                            const capabilities = RTCRtpReceiver.getCapabilities('video');
                            if (capabilities && capabilities.codecs) {
                                // 컴퓨터가 지원하는 코덱 리스트 중에서 H.264만 뽑아냅니다.
                                const h264Codecs = capabilities.codecs.filter(c => c.mimeType === 'video/H264');
                                if (h264Codecs.length > 0) {
                                    try {
                                        transceiver.setCodecPreferences(h264Codecs); // H.264 최우선 협상
                                        console.log(`[Peer] H.264 Hardware Encoder preference set for ${peerId}`);
                                    } catch (e) {
                                        console.error('H264 preference failed', e);
                                    }
                                }
                            }
                        }


                        // <코덱 설정>

                        /*
                        // [2] 하드웨어 가속기(HW Encoder)를 최우선으로 깨우도록 코덱 리스트 정렬
                        const transceiver = pc.getTransceivers().find(t => t.sender === sender);
                        if (transceiver && 'setCodecPreferences' in transceiver) {
                            const capabilities = RTCRtpReceiver.getCapabilities('video');
                            if (capabilities && capabilities.codecs) {

                                // 컴퓨터가 지원하는 전체 코덱 리스트를 가져와 선호하는 코덱을 맨 위로 올림
                                const sortedCodecs = [...capabilities.codecs].sort((a, b) => {
                                    const isTargetA = a.mimeType === targetMimeType;
                                    const isTargetB = b.mimeType === targetMimeType;

                                    // a나 b가 내가 원하는 1순위인지 확인하고, 1순위인 것을 앞으로 정렬
                                    if (isTargetA && !isTargetB) return -1;
                                    if (!isTargetA && isTargetB) return 1;
                                    return 0;
                                });

                                try {
                                    // 선호 코덱 최우선 협상, 상대가 미지원 시 배열 뒤쪽의 코덱들로 폴백(Fallback) 허용
                                    transceiver.setCodecPreferences(sortedCodecs);
                                    console.log(`[Peer] VP9 Hardware Encoder preference sorted (Target on top) for ${peerId}`);
                                } catch (e) {
                                    console.error('Codec preference failed', e);
                                }
                            }
                        }
                        */

                    }
                });
            }
            else {
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
            }
        };

        pc.onicecandidateerror = (e) => {
            const err = e as RTCPeerConnectionIceErrorEvent;
            console.warn('ICE error', err.errorCode, err.errorText, err.url);
        };


        pc.onconnectionstatechange = async () => {
            console.log(`[${peerId}] state:`, pc.connectionState);

            if (pc.connectionState === 'disconnected') {
                console.log(`[${peerId}] Connection ${pc.connectionState}.`);

                // 재연결 시도 join-redial for recvonly connections
                const pcType = pcTypesRef.current[peerId];
                console.log(`[${peerId}] Connection disconnected. Attempting renegotiate.`);
                if (pcType === 'recvonly') {
                    // const targetPc = pcsRef.current[peerId];
                    console.log(`[${peerId}] recvonly connection lost. Attempting renegotiate.`);
                    renegotiateSamePc(peerId);
                }
            }
            else if (pc.connectionState === 'failed' || pc.connectionState === 'closed') {
                // 재연결 시도 redial
                console.log(`[${peerId}] Connection failed. Attempting to redial ICE...`);
                const targetPc = pcsRef.current[peerId];
                const pcType = pcTypesRef.current[peerId];

                // PC cleanup 공통 로직
                if (targetPc) {
                    targetPc.close();
                }
                delete pcsRef.current[peerId];
                delete pcTypesRef.current[peerId];
                pendingCandRef.current[peerId] = [];
                iceCandidateGatheredArrayRef.current[peerId] = [];
                redialCountsRef.current[peerId] = (redialCountsRef.current[peerId] || 0) + 1;
                setUsers(prev => prev.filter(u => u.id !== peerId));

                if (pcType === 'recvonly') {
                    if (redialCountsRef.current[peerId] > MAX_REDIAL_ATTEMPTS) {
                        console.log(`[${peerId}] Max redial attempts reached. Not attempting further redials.`);
                        return;
                    }
                    else {
                        console.log(`[${peerId}] recvonly connection lost. Attempting redial.${redialCountsRef.current[peerId]}`);
                        // 1:N과 그대로지만, payload에 to가 없기 때문에 서버에서 myid로 처리됨 
                        socketRef.current?.emit('join', { room: room, type: 'redial' });
                    }
                }
                else {
                    console.log(`[${peerId}] Non-recvonly connection failed. Closing peer connection.`);
                }

            }
            else if (pc.signalingState === 'closed') {
                console.log(`[${peerId}] Signaling state closed.`);
            }
            else if (pc.iceConnectionState === 'closed') {
                console.log(`[${peerId}] ICE connection state closed.`);
            }
            else if (pc.connectionState === 'connected') {
                console.log(`[${peerId}] Connection established successfully.changeCount:${changeCount}`);
                redialCountsRef.current[peerId] = 0; // 재연결 성공 시 카운트 초기화
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
            }
        }


        return pc;
    }, [socketRef.current]); // 의존성 배열이 비어있으므로 이 함수는 컴포넌트가 처음 렌더링될 때 한 번만 생성

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

    }, []);

    // 대기 중인 ICE Candidate들을 일괄 처리하는 함수 <DG>
    // RemoteDescription이 설정되기 전에 도착한 Candidate들은 버퍼(pendingCandRef)에 저장된다. RemoteDescription 설정이 완료된 직후 이 함수를 호출하여 버퍼에 쌓인 Candidate들을 PeerConnection에 등록한다.
    const flushPendingCandidates = async (peerId: string) => {

        const pc = pcsRef.current[peerId]; // 해당 peerId에 매핑된 RTCPeerConnection 객체 가져오기
        const pendingCandidates = pendingCandRef.current[peerId]; // 해당 peerId에 대해 버퍼링된 ICE Candidate 목록 가져오기

        // PeerConnection이 존재하고, 처리해야 할 대기열(pendingCandidates)이 있을 경우에만 실행
        if (pc && pendingCandidates && pendingCandidates.length > 0) {
            console.log(`[Peer] Flushing ${pendingCandidates.length} candidates for ${peerId}`);

            // 대기 중인 모든 Candidate RTCPeerConnection에 추가
            for (const candidate of pendingCandidates) {
                try {
                    await pc.addIceCandidate(new RTCIceCandidate(candidate));
                } catch (e) {
                    console.warn(`[Peer] Failed to add buffered candidate`, e);
                }
            }
            // 모든 처리가 완료되면 버퍼를 비워 중복 처리를 방지
            pendingCandRef.current[peerId] = [];
        }
    };

    // 동일 PC 기반 renegotiation: iceRestart + offer 재생성 (pc 유지)
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

            // 처음 설정한 SDP bandwidth 제한 로직 유지 (방장 한정)
            let finalSdp: any = offer;
            if (isRootRef.current) {
                const newSdp = setMaxBandwidth(offer.sdp || '', 'video', BITRATE);
                finalSdp = newSdp ? { type: offer.type, sdp: newSdp } : offer;
            }

            await pc.setLocalDescription(finalSdp);

            // 서버로 offer 전송
            socket.emit('offer-renegotiate', { to: peerId, data: finalSdp });
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

            // 연결 강제 종료
            pc.close();
        } catch (e) {
            console.error(`[forceDisconnectPeer] error closing pc for ${peerId}`, e);
        }

        // 레퍼런스/상태 정리
        delete pcsRef.current[peerId];
        delete pcTypesRef.current[peerId];
        if (pendingCandRef.current) pendingCandRef.current[peerId] = [];

        // setUsers(prev => prev.filter(u => u.id !== peerId));

        console.log(`[forceDisconnectPeer] disconnected peerId=${peerId}`);
        return true;
    };
    forceDisconnectPeerRef.current = forceDisconnectPeer;
    // 방 재접속: 전 피어 종료 + ref/state 초기화 + 소켓 재연결
    const reset = useCallback(async () => {
        console.log(`[RESET] Disconnecting all ${Object.keys(pcsRef.current).length} peers...`);
        // 시그널링 서버에 연결 종료 알림
        socketRef.current?.emit('disconnect_reset');
        // 소켓 연결 종료
        if (socketRef.current) {
            socketRef.current.disconnect();
            // socketRef.current = null; // 필요 시 제거
        }
        const peerIds = Object.keys(pcsRef.current);
        let disconnectedCount = 0;

        peerIds.forEach(peerId => {
            if (forceDisconnectPeerRef.current(peerId)) {
                disconnectedCount++;
            }
        });

        // pcsRef 초기화
        pcsRef.current = {};
        pcTypesRef.current = {};
        pendingCandRef.current = {};
        iceCandidateGatheredArrayRef.current = {};
        redialCountsRef.current = {};
        // 사용자 목록 초기화
        setUsers([]);
        console.log(`[RESET] Successfully disconnected ${disconnectedCount} peers`);
        // 방속 접속 재접속 전 3s 지연 ; 너무 빨리 접속 되기때문에 임의로 지연 추가
        await new Promise(resolve => setTimeout(resolve, 3000));
        socketRef.current?.connect(); // 스트림 획득 후 소켓 연결
        return disconnectedCount;
    }, []);

    useEffect(() => {
        // 디버그용 전역 노출
        (window as any).forceDisconnectPeer = (peerId: string) => {
            return forceDisconnectPeerRef.current(peerId);
        };

        // (선택) 현재 pcsRef도 보고 싶으면 같이 노출
        (window as any).pcsRef = pcsRef;
        (window as any).disconnectAllPeers = reset;

        return () => {
            delete (window as any).forceDisconnectPeer;
            delete (window as any).pcsRef;
            delete (window as any).disconnectAllPeers;
        };
    }, [forceDisconnectPeerRef, reset]);


    return (
        <div style={{ padding: 16, fontFamily: "system-ui, sans-serif" }}>
            <h2>WebRTC Peer (React)</h2>

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
                <button onClick={() => reset()} style={{ marginLeft: 8, backgroundColor: '#ff4444', color: 'white', padding: '6px 12px', border: 'none', borderRadius: '4px', cursor: 'pointer' }}>RESET</button>
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
            {notice && (
                <div style={{
                    position: 'fixed',
                    top: 20,
                    left: '50%',
                    transform: 'translateX(-50%)',
                    background: 'rgba(0,0,0,0.85)',
                    color: '#fff',
                    padding: '10px 16px',
                    borderRadius: 8,
                    zIndex: 10000,
                    fontSize: 16
                }}>
                    {notice}
                </div>
            )}
        </div>

    );
}

export default App;