import path from "path";

const { google } = require('googleapis');
const fs = require('fs');
// Authenticate with the service account
const auth = new google.auth.GoogleAuth({
  keyFile: './inner-legacy-284818-9936a92f5ecf.json',
  scopes: ['https://www.googleapis.com/auth/drive'],
});
const driveService = google.drive({ version: 'v3', auth });

export const listFilesInFolder = async (folderId: string) => {
  try {
    let filesData = [];
    const res = await driveService.files.list({
      q: `'${folderId}' in parents`,
      fields: 'files(id, name)',
    });
    const files = res.data.files;
    if (files.length === 0) {
      console.log('No files found.');
    } else {
      console.log('Files:');
      filesData = files;
      files.forEach((file: any) => {
        console.log(`${file.name} (${file.id})`);
      });
    }
    return filesData;
  } catch (error) {
    console.error('Error listing files:', error);
  }
}

export const downloadFileFromDrive = async (fileId: string) => {
  try {
    const res = await driveService.files.get(
      { fileId: fileId, alt: 'media' },
      { responseType: 'stream' }
    );
    const metadataRes = await driveService.files.get({
      fileId: fileId,
      fields: 'name', // Fetch only the name field
    });
    const fileName = metadataRes.data.name;
    console.log(`File name: ${fileName}`);
    let filePath = path.join(__dirname, `../../.sandbox/${fileName}`); // Path to and name of object. For example '../myFiles/index.js'.
    const dest = fs.createWriteStream(filePath);
    await new Promise<void>((resolve, reject) => {
      res.data
        .on('end', () => {
          console.log(`Downloaded file to ${'filename.csv'}`);
          resolve();
        })
        .on('error', (err: any) => {
          console.error('Error downloading file:', err);
          reject(err);
        })
        .pipe(dest);
    });
    return {orgFileName: fileName, errorFlag: null, errorType: null}
    // return filename;
  } catch (error: any) {
    console.error(" Error in download file", error.message)
    return {orgFileName: 'fileName', errorFlag: null, errorType: null}
  }
}

export const uploadFileToGoogleFolder = async (
  callbackUrl: string,
  fileName: string,
  email: string,
  filePath: string
) => {
  try {

    // Parse the CSV file and get the data
    const destinationAddress = callbackUrl
    const folderIdMatch = destinationAddress.match(/\/folders\/([a-zA-Z0-9-_]+)/);
    const folderId =  folderIdMatch ? folderIdMatch[1] : null;

    if(folderId){
      const fileMetadata = {
        name: fileName,
        parents: [folderId],
      };
      const media = {
        mimeType: 'text/csv', // Use text/csv for CSV files
        body: fs.createReadStream(filePath),
      };
      const res = await driveService.files.create({
        resource: fileMetadata,
        media: media,
        fields: 'id',
      });
      console.log(`Uploaded file ID: ${res.data.id}`);
    }
    console.log('File uploaded to folder successfully');

  } catch (error) {
    console.error('Error updating updateHubspotContacts:', error.message);
  }
};