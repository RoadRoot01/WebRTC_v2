/**
 * VideoFrame-based Insertable Streams overlay.
 *
 * The encoded transform API cannot draw on encoded video data directly, so this
 * module uses MediaStreamTrackProcessor/MediaStreamTrackGenerator to edit each
 * raw VideoFrame before it is sent to a peer.
 */

export interface RedBoxOverlayPipeline {
    stream: MediaStream;
    stop: () => void;
}

const RED_BOX_MARGIN = 16;
const RED_BOX_WIDTH = 96;
const RED_BOX_HEIGHT = 64;

/**
 * Returns a stream whose first video track has a red rectangle composited in
 * the upper-left corner. Audio and non-video tracks are passed through.
 * If Insertable Streams are unavailable, the original stream is returned.
 */
export function createRedBoxOverlayStream(sourceStream: MediaStream): RedBoxOverlayPipeline {
    const videoTrack = sourceStream.getVideoTracks()[0];
    const Processor = (window as any).MediaStreamTrackProcessor;
    const Generator = (window as any).MediaStreamTrackGenerator;

    if (!videoTrack || !Processor || !Generator || typeof OffscreenCanvas === 'undefined') {
        console.warn('[Insertable Streams] VideoFrame processing is not supported; sending the original stream.');
        return { stream: sourceStream, stop: () => undefined };
    }

    const processor = new Processor({ track: videoTrack });
    const generator = new Generator({ kind: 'video' });
    const abortController = new AbortController();
    let stopped = false;

    const transform = new TransformStream({
        transform: (frame: any, controller) => {
            const width = frame.displayWidth || frame.codedWidth;
            const height = frame.displayHeight || frame.codedHeight;
            const canvas = new OffscreenCanvas(width, height);
            const context = canvas.getContext('2d') as OffscreenCanvasRenderingContext2D | null;

            if (!context) {
                frame.close();
                return;
            }

            context.drawImage(frame, 0, 0, width, height);
            context.fillStyle = '#ff0000';
            context.fillRect(RED_BOX_MARGIN, RED_BOX_MARGIN, RED_BOX_WIDTH, RED_BOX_HEIGHT);

            const compositedFrame = new (window as any).VideoFrame(canvas, {
                timestamp: frame.timestamp,
                duration: frame.duration,
            });
            frame.close();
            controller.enqueue(compositedFrame);
        },
    });

    processor.readable
        .pipeThrough(transform, { signal: abortController.signal })
        .pipeTo(generator.writable, { signal: abortController.signal })
        .catch((error: unknown) => {
            if (!stopped) {
                console.error('[Insertable Streams] Red-box overlay pipeline failed.', error);
            }
        });

    const processedStream = new MediaStream([
        generator,
        ...sourceStream.getAudioTracks(),
        ...sourceStream.getTracks().filter(track => track.kind !== 'video' && track.kind !== 'audio'),
    ]);

    return {
        stream: processedStream,
        stop: () => {
            if (stopped) return;
            stopped = true;
            abortController.abort();
            generator.stop();
            sourceStream.getTracks().forEach(track => track.stop());
        },
    };
}
