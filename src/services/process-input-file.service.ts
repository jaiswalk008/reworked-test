import { /* inject, */ BindingScope, injectable} from '@loopback/core';
import {spawn} from "child_process";
import {getExecutePythonCommand} from '../config';

export async function runPythonScript(python_script_full_path: string, python_args?: Array<any>) {
  return new Promise((resolve, reject) => {
  
    const outputArray: Array<Buffer> = [];
    const python = spawn(getExecutePythonCommand(), python_args ? [python_script_full_path, ...python_args] : [python_script_full_path]);

    python.stdout.on('data', (chunk: any) => {
      console.log(`data ... ${chunk.toString()}`);
      outputArray.push(Buffer.from(chunk));
    });

    python.stdout.on('error', (data: Buffer) => {
      console.error('stdout error ...', data.toString());
      //reject();
      reject(data);
    });

    python.stderr.on('data', (data: Buffer) => {
      console.error('stderr  data...', data.toString());
      //reject();
      outputArray.push(Buffer.from(data));
    });

    // in close event we are sure that stream from child process is closed
    python.on('close', () => {
      console.log(`PY-RUN: onClose -> $`);
      resolve(Buffer.concat(outputArray).toString().trim().split("\n"));
    });

  });
}

@injectable({scope: BindingScope.TRANSIENT})
export class ProcessInputFileService {
  constructor(fileName: String, options: Object) {
    console.log(fileName);

  }



  /*
   * Add service methods here
   */
}
