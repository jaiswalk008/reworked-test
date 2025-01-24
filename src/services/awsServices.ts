
import s3 from './s3Client'
import path from "path";
import fs from "fs";


export async function UploadS3(key: string, body: fs.ReadStream, userFolder: string = 'test') {
    const uploadParams = {
        Bucket: process.env.S3_BUCKET_NAME || '',
        // Add the required 'Key' parameter using the 'path' module.
        Key: userFolder + "/" + path.basename(key),
        // Add the required 'Body' parameter
        Body: body,
    };

    const uploadResponse = await s3.upload(uploadParams).promise()
    return uploadResponse
}

export async function DownloadS3(key: string, writer: fs.WriteStream) {
    const downloadParams = {
        Bucket: process.env.S3_BUCKET_NAME || '',
        // Add the required 'Key' parameter using the 'path' module.
        Key: key
    };
    try {
        const downloadResponse = s3.getObject(downloadParams).createReadStream()
        downloadResponse.pipe(writer);
    } catch (e) {
        throw new Error(e.message)
    }

}

export async function downloadFileFromS3(filename: string, email: string, expiry: number = 300) {
    const params = {
        Bucket: process.env.S3_BUCKET_NAME || '',
        Key: email + '/' + filename,
        Expires: expiry, // the URL will expire in 5 mins if not provided
    };

    try {
        const url = await s3.getSignedUrlPromise('getObject', params)
        return url
    } catch (e) {
        throw new Error(e.message)
    }


}
export async function checkS3ObjectExistence(s3Key: string) {
    try {
        const params = {
            Bucket: process.env.S3_BUCKET_NAME || '',
            Key: s3Key,
        };
        // Check if the object exists
        await s3.headObject(params).promise();
        // If the object exists, return true
        return true;
    } catch (error) {
        return false;
        // // If the object doesn't exist or there's an error, return false
        // if (error.code === 'NotFound') {
        //     return false;
        // } else {
        //     false
        //     throw error; // Throw other errors for handling elsewhere
        // }
    }
}


export async function ListS3() {
    const downloadParams = {
        Bucket: process.env.S3_BUCKET_NAME || '',
        // Add the required 'Key' parameter using the 'path' module.
    };
    const listResponse = await s3.listObjects(downloadParams).promise();
    return listResponse;
}


export const downloadFileFromS3IfNotAvailable = async (filePath: string, email: string) => {
    try {
        if (!fs.existsSync(filePath)) {
            // const toWriteStream = fs.createWriteStream(filePath);
            // toWriteStream.on('finish', () => {
            //     console.log('File download completed.');
            // });
            // toWriteStream.on('error', (err) => {
            //     console.error('Error writing to local file:', err);
            // });

            const key = `${email}/${path.basename(filePath)}`;
            const downloadParams = {
                Bucket: process.env.S3_BUCKET_NAME || '',
                Key: key,
            };

            // const downloadResponse = await s3.getObject(downloadParams).promise();
            // if (downloadResponse.Body) {
            //     // Write the S3 object's data to the local file
            //     toWriteStream.write(downloadResponse.Body);
            //     toWriteStream.end();
            // } else {
            //     console.error('S3 response body is undefined.');
            // }
            
            let content = await (await s3.getObject(downloadParams).promise()).Body;

            
            if (content instanceof Buffer) {
                try {
                    fs.writeFileSync(filePath, content);
                    console.log("file downloaded", filePath)
                } catch (err) {
                    console.error(err);
                }
                // fs.writeFileSync(filePath, content);

                // fs.writeFile(filePath, content, (err) => {
                //     if (err) { console.log(err); }
                // });
            }
        } else {
            console.log('File already exists locally.');
        }
    } catch (e) {
        console.error('Error:', e.message);
    }
};


export const downloadFilesFromS3 = async (filePaths: any, email: string) => {

    const downloadPromises = filePaths.map((filePath: any) => downloadFileFromS3IfNotAvailable(filePath, email));

    try {
        await Promise.all(downloadPromises);
        console.log('All files downloaded successfully.');
    } catch (error) {
        console.error('An error occurred while downloading files:', error);
    }
}

export const generatePresignedS3Url = (key: string, userFolder: string = 'test', expireTime: number = 3600) => {
    // Generate a presigned URL for the uploaded object
    const presignedUrl = s3.getSignedUrl('getObject', {
        Bucket: process.env.S3_BUCKET_NAME || '',
        Key: userFolder + "/" + path.basename(key),
        Expires: expireTime, // Presigned URL expiration time in seconds
    });
    return presignedUrl
}