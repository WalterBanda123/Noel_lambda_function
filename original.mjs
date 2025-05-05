import { S3 } from '@aws-sdk/client-s3';
import sharp from 'sharp';

// Load formats from environment variables or use defaults
const SUPPORTED_FORMATS = {
    images: {
        extensions: process.env.SUPPORTED_IMAGE_EXTENSIONS?.split(',') || ['jpg', 'jpeg', 'png', 'webp'],
        mimeTypes: process.env.SUPPORTED_IMAGE_MIMETYPES?.split(',') || ['image/jpeg', 'image/png', 'image/webp'],
        sharpFormats: {
            'jpg': 'jpeg',
            'jpeg': 'jpeg',
            'png': 'png',
            'webp': 'webp'
        }
    },
    videos: {
        extensions: process.env.SUPPORTED_VIDEO_EXTENSIONS?.split(',') || ['mp4', 'mov', 'avi', 'mkv'],
        mimeTypes: process.env.SUPPORTED_VIDEO_MIMETYPES?.split(',') || ['video/mp4', 'video/quicktime', 'video/x-msvideo', 'video/x-matroska']
    }
};

const validateConfig = () => {
    const requiredEnvVars = [
        'SOURCE_BUCKET',
        'SOURCE_PREFIX',
        'LOW_BUCKET',
        'LOW_FOLDER',
        'THUMB_BUCKET',
        'THUMB_FOLDER',
        'REGION'
    ];

    const missingVars = requiredEnvVars.filter(envVar => !process.env[envVar]);
    if (missingVars.length > 0) {
        throw new Error(`Missing required environment variables: ${missingVars.join(', ')}`);
    }

    return {
        sourceBucket: process.env.SOURCE_BUCKET,
        sourcePrefix: process.env.SOURCE_PREFIX,
        region: process.env.REGION,
        cdnDomain: process.env.CDN_DOMAIN || null,
        sizes: {
            low: {
                width: parseInt(process.env.LOW_WIDTH || '800'),
                bucket: process.env.LOW_BUCKET,
                folder: ensureTrailingSlash(process.env.LOW_FOLDER)
            },
            thumb: {
                width: parseInt(process.env.THUMB_WIDTH || '150'),
                bucket: process.env.THUMB_BUCKET,
                folder: ensureTrailingSlash(process.env.THUMB_FOLDER)
            }
        }
    };
};

const ensureTrailingSlash = (path) => {
    return path.endsWith('/') ? path : `${path}/`;
};

class MediaProcessor {
    constructor() {
        this.CONFIG = validateConfig();
        this.s3 = new S3({ region: this.CONFIG.region });
    }

    async getSourceMetadata(key) {
        try {
            const headObject = await this.s3.headObject({
                Bucket: this.CONFIG.sourceBucket,
                Key: key
            });

            return {
                contentType: headObject.ContentType,
                metadata: headObject.Metadata || {},
                lastModified: headObject.LastModified,
                contentLength: headObject.ContentLength,
                eTag: headObject.ETag
            };
        } catch (error) {
            console.error(`Error getting source metadata for ${key}:`, error);
            throw new Error(`Failed to get source metadata: ${error.message}`);
        }
    }

    generateResourcePath(bucket, key) {
        if (this.CONFIG.cdnDomain) {
            const cleanKey = key.replace(/^\/+/, '');
            return `${this.CONFIG.cdnDomain}/${cleanKey}`;
        }
        return `https://${bucket}.s3.${this.CONFIG.region}.amazonaws.com/${key}`;
    }

    validateFormat(key) {
        if (!key || typeof key !== 'string') {
            throw new Error('Invalid key: Key must be a non-empty string');
        }

        const extension = key.split('.').pop().toLowerCase();
        if (!extension) {
            throw new Error(`Invalid file format: No extension found in key "${key}"`);
        }

        const isImage = SUPPORTED_FORMATS.images.extensions.includes(extension);
        const isVideo = SUPPORTED_FORMATS.videos.extensions.includes(extension);

        if (!isImage && !isVideo) {
            throw new Error(
                `Unsupported format "${extension}". Supported formats are: ` +
                `Images: ${SUPPORTED_FORMATS.images.extensions.join(', ')}, ` +
                `Videos: ${SUPPORTED_FORMATS.videos.extensions.join(', ')}`
            );
        }

        return {
            format: extension,
            type: isImage ? 'image' : 'video'
        };
    }

    async processImage(key, format, width, destinationBucket, destinationFolder) {
        console.log(`Processing image: ${key} to width: ${width}, format: ${format}, destination: ${destinationBucket}/${destinationFolder}`);

        try {
            const sourceMetadata = await this.getSourceMetadata(key);

            // Get the source image
            const { Body: sourceStream } = await this.s3.getObject({
                Bucket: this.CONFIG.sourceBucket,
                Key: key
            });

            if (!sourceStream) {
                throw new Error('Source stream is null');
            }

            // Read the entire stream into a buffer
            const chunks = [];
            for await (const chunk of sourceStream) {
                chunks.push(chunk);
            }
            const imageBuffer = Buffer.concat(chunks);

            // Initialize sharp with the buffer
            let sharpInstance = sharp(imageBuffer, {
                failOnError: true,
                animated: true // Preserve animation for formats that support it
            });

            // Get image metadata
            const metadata = await sharpInstance.metadata();
            console.log('Original image metadata:', metadata);

            // Resize the image
            sharpInstance = sharpInstance.resize(width, null, {
                withoutEnlargement: true,
                fit: 'inside'
            });

            // Preserve transparency for PNG
            if (format.toLowerCase() === 'png') {
                sharpInstance = sharpInstance.png({
                    compressionLevel: 9,
                    palette: true
                });
            } else if (format.toLowerCase() === 'jpeg' || format.toLowerCase() === 'jpg') {
                sharpInstance = sharpInstance.jpeg({
                    quality: 80,
                    mozjpeg: true
                });
            } else if (format.toLowerCase() === 'webp') {
                sharpInstance = sharpInstance.webp({
                    quality: 80,
                    lossless: false
                });
            }

            // Process the image
            const resizedImageBuffer = await sharpInstance.toBuffer();

            // Generate the new key maintaining the original format
            const originalFilename = key.split('/').pop();
            const filenameWithoutExt = originalFilename.substring(0, originalFilename.lastIndexOf('.'));
            const newKey = `${destinationFolder}${filenameWithoutExt}.${format.toLowerCase()}`;

            console.log(`Uploading processed image to: ${destinationBucket}/${newKey}`);

            // Upload to S3
            const uploadParams = {
                Bucket: destinationBucket,
                Key: newKey,
                Body: resizedImageBuffer,
                ContentType: `image/${format.toLowerCase() === 'jpg' ? 'jpeg' : format.toLowerCase()}`,
                Metadata: {
                    ...sourceMetadata.metadata,
                    originalKey: key,
                    processedAt: new Date().toISOString(),
                    processingWidth: width.toString(),
                    originalFormat: format,
                    outputFormat: format
                }
            };

            await this.s3.putObject(uploadParams);
            console.log(`Successfully uploaded processed image: ${newKey} to bucket: ${destinationBucket}`);

            // Verify the upload
            const uploadedObject = await this.s3.headObject({
                Bucket: destinationBucket,
                Key: newKey
            });

            if (!uploadedObject) {
                throw new Error('Failed to verify uploaded object');
            }

            return {
                key: newKey,
                bucket: destinationBucket,
                url: this.generateResourcePath(destinationBucket, newKey),
                metadata: {
                    ...sourceMetadata,
                    processedWidth: width,
                    processedAt: new Date().toISOString(),
                    originalFormat: format,
                    outputFormat: format
                }
            };
        } catch (error) {
            console.error(`Error processing image ${key}:`, error);
            throw new Error(`Failed to process image: ${error.message}`);
        }
    }

    async processVideo(key, format) {
        console.log(`Processing video: ${key}`);

        try {
            const sourceMetadata = await this.getSourceMetadata(key);
            const filename = key.replace(this.CONFIG.sourcePrefix, '');
            const newKey = `${this.CONFIG.sizes.low.folder}${filename}`;

            const copyParams = {
                Bucket: this.CONFIG.sizes.low.bucket,
                CopySource: `${this.CONFIG.sourceBucket}/${key}`,
                Key: newKey,
                MetadataDirective: 'COPY'
            };

            await this.s3.copyObject(copyParams);

            // Verify the copy
            const copiedObject = await this.s3.headObject({
                Bucket: this.CONFIG.sizes.low.bucket,
                Key: newKey
            });

            if (!copiedObject) {
                throw new Error('Failed to verify copied video object');
            }

            console.log(`Successfully copied video: ${newKey} to bucket: ${this.CONFIG.sizes.low.bucket}`);

            return {
                key: newKey,
                bucket: this.CONFIG.sizes.low.bucket,
                url: this.generateResourcePath(this.CONFIG.sizes.low.bucket, newKey),
                metadata: {
                    ...sourceMetadata,
                    copiedAt: new Date().toISOString()
                }
            };
        } catch (error) {
            console.error(`Error processing video ${key}:`, error);
            throw new Error(`Failed to process video: ${error.message}`);
        }
    }

    validateEvent(event) {
        if (!event) {
            throw new Error('Event object is undefined');
        }

        if (!Array.isArray(event.Records) || event.Records.length === 0) {
            throw new Error('Event contains no records');
        }

        const record = event.Records[0];
        if (!record.s3 || !record.s3.object || !record.s3.object.key) {
            throw new Error('Invalid S3 event structure');
        }

        return record;
    }

    
    async handleResize(event) {
        try {
            const record = this.validateEvent(event);
            const key = decodeURIComponent(
                record.s3.object.key.replace(/\+/g, ' ')
            );

            console.log(`Received request to process: ${key}`);

            const { format, type } = this.validateFormat(key);
            let processingResult = {};

            if (type === 'image') {
                console.log('Processing image with format:', format);

                // Process both sizes sequentially to avoid memory issues
                const lowQuality = await this.processImage(
                    key,
                    format,
                    this.CONFIG.sizes.low.width,
                    this.CONFIG.sizes.low.bucket,
                    this.CONFIG.sizes.low.folder
                );

                const thumbnail = await this.processImage(
                    key,
                    format,
                    this.CONFIG.sizes.thumb.width,
                    this.CONFIG.sizes.thumb.bucket,
                    this.CONFIG.sizes.thumb.folder
                );

                processingResult = {
                    success: true,
                    data: {
                        paths: {
                            original: this.generateResourcePath(this.CONFIG.sourceBucket, key),
                            thumbnail: thumbnail.url,
                            lowQuality: lowQuality.url
                        },
                        metadata: {
                            type: 'image',
                            format: format,
                            processedAt: new Date().toISOString(),
                            originalKey: key,
                            thumbnailKey: thumbnail.key,
                            lowQualityKey: lowQuality.key
                        }
                    }
                };
            } else if (type === 'video') {
                const videoResult = await this.processVideo(key, format);
                // ... (video processing remains the same)
            }

            return {
                statusCode: 200,
                body: JSON.stringify(processingResult)
            };
        } catch (error) {
            console.error('Processing error:', error);
            const statusCode = error.message.includes('Unsupported format') ? 400 : 500;

            return {
                statusCode,
                body: JSON.stringify({
                    success: false,
                    error: error.message,
                    stack: process.env.NODE_ENV === 'development' ? error.stack : undefined
                })
            };
        }
    }
}

export const handler = async (event) => {
    try {
        const processor = new MediaProcessor();
        return await processor.handleResize(event);
    } catch (error) {
        console.error('Lambda handler error:', error);
        return {
            statusCode: 500,
            body: JSON.stringify({
                success: false,
                error: error.message,
                stack: process.env.NODE_ENV === 'development' ? error.stack : undefined
            })
        };
    }
};