import Process from 'process';

export const getExecutePythonCommand = () => Process.env.EXECUTE_PYTHON_COMMAND ?? 'python3';
