import { S3Client, CopyObjectCommand, HeadObjectCommand } from '@aws-sdk/client-s3';

const s3Client = new S3Client({ region: process.env.AWS_REGION });

const SIZES = {
  MLS: {
    folder: 'mls',
    width: 2048
  },
  THUMB: {
    folder: 'thumb',
    width: 240
  }
};

export const handler = async (event) => {
  try {
    const record = event.Records[0].s3;
    const bucket = record.bucket.name;
    const key = decodeURIComponent(record.object.key.replace(/\+/g, ' '));

    // Validate key format: media/{propertyId}/original/{filename}
    const keyParts = key.split('/');
    if (keyParts.length !== 4 || keyParts[0] !== 'media' || keyParts[2] !== 'original') {
      console.log(`Skipping invalid key format: ${key}`);
      return {
        statusCode: 200,
        body: JSON.stringify({ message: 'Skipped - invalid key format' })
      };
    }

    const propertyId = keyParts[1];
    const filename = keyParts[3];

    // Get original file metadata
    const headCommand = new HeadObjectCommand({
      Bucket: bucket,
      Key: key
    });
    
    const { ContentType, Metadata } = await s3Client.send(headCommand);

    // Create copies for each size
    const results = await Promise.all(
      Object.entries(SIZES).map(async ([size, config]) => {
        const newKey = `media/${propertyId}/${config.folder}/${filename}`;
        
        const copyCommand = new CopyObjectCommand({
          Bucket: bucket,
          Key: newKey,
          CopySource: `${bucket}/${key}`,
          ContentType,
          Metadata: {
            ...Metadata,
            processedAt: new Date().toISOString(),
            processingSize: size,
            originalKey: key,
            targetWidth: config.width.toString()
          },
          MetadataDirective: 'REPLACE'
        });

        await s3Client.send(copyCommand);
        
        return {
          size,
          key: newKey,
          url: `https://${bucket}.s3.${process.env.AWS_REGION}.amazonaws.com/${newKey}`
        };
      })
    );

    console.log('Successfully processed:', {
      original: key,
      copies: results
    });

    return {
      statusCode: 200,
      body: JSON.stringify({
        message: 'Processing completed successfully',
        results
      })
    };
  } catch (error) {
    console.error('Lambda handler error:', error);
    return {
      statusCode: 500,
      body: JSON.stringify({
        error: 'Failed to process image',
        details: error.message
      })
    };
  }
};

