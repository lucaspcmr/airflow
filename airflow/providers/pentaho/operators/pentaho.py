from base64 import b64encode
from select import select
from typing import Optional, Union

from airflow.configuration import conf
from airflow.exceptions import AirflowException
from airflow.models import BaseOperator
from airflow.providers.ssh.hooks.ssh import SSHHook
from airflow.utils.decorators import apply_defaults


class PentahoKitchenOperator(BaseOperator):
    """
    SSHOperator to execute commands on given remote host using the ssh_hook.

    :param ssh_hook: predefined ssh_hook to use for remote execution.
        Either `ssh_hook` or `ssh_conn_id` needs to be provided.
    :type ssh_hook: airflow.providers.ssh.hooks.ssh.SSHHook
    :param ssh_conn_id: connection id from airflow Connections.
        `ssh_conn_id` will be ignored if `ssh_hook` is provided.
    :type ssh_conn_id: str
    :param remote_host: remote host to connect (templated)
        Nullable. If provided, it will replace the `remote_host` which was
        defined in `ssh_hook` or predefined in the connection of `ssh_conn_id`.
    :type remote_host: str
    :param job_path: job path to execute on remote host.
    :type job: str
    :param log_level: level of kitchen log
    :type str
    :param log_path path to store log on remote host
    :type str
    :param kitchen_path: path to kitchen.sh on remote host
    :type str
    :param extra_params: extra params to use in job
    :type list
    :param extra_args: extra args to use in job
    :param timeout: timeout (in seconds) for executing the command. The default is 10 seconds.
    :type timeout: int
    :param environment: a dict of shell environment variables. Note that the
        server will reject them silently if `AcceptEnv` is not set in SSH config.
    :type environment: dict
    :param get_pty: request a pseudo-terminal from the server. Set to ``True``
        to have the remote process killed upon task timeout.
        The default is ``False`` but note that `get_pty` is forced to ``True``
        when the `command` starts with ``sudo``.
    :type get_pty: bool
    """

    template_fields = ('remote_host',)
    template_ext = ('.sh',)

    @apply_defaults
    def __init__(
        self,
        *,
        ssh_hook: Optional[SSHHook] = None,
        ssh_conn_id: Optional[str] = None,
        remote_host: Optional[str] = None,
        job_path: str = None,
        log_level: Optional[str] = 'Basic',
        log_path: Optional[str] = None,
        kitchen_path: Optional[str] = None,
        extra_params: Optional[list] = None,
        extra_args: Optional[list] = None,
        timeout: int = 10,
        environment: Optional[dict] = None,
        get_pty: bool = False,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.ssh_hook = ssh_hook
        self.ssh_conn_id = ssh_conn_id
        self.remote_host = remote_host
        self.job_path = job_path
        self.log_level = log_level
        self.log_path = log_path
        self.kitchen_path = kitchen_path
        self.extra_params = extra_params
        self.extra_args = extra_args
        self.timeout = timeout
        self.environment = environment
        self.get_pty = (self.job_path.startswith('sudo') or get_pty) if self.job_path else get_pty

    def execute(self, context) -> Union[bytes, str, bool]:
        try:
            if self.ssh_conn_id:
                if self.ssh_hook and isinstance(self.ssh_hook, SSHHook):
                    self.log.info("ssh_conn_id is ignored when ssh_hook is provided.")
                else:
                    self.log.info(
                        "ssh_hook is not provided or invalid. Trying ssh_conn_id to create SSHHook."
                    )
                    self.ssh_hook = SSHHook(ssh_conn_id=self.ssh_conn_id, timeout=self.timeout)

            if not self.ssh_hook:
                raise AirflowException("Cannot operate without ssh_hook or ssh_conn_id.")

            if self.remote_host is not None:
                self.log.info(
                    "remote_host is provided explicitly. "
                    "It will replace the remote_host which was defined "
                    "in ssh_hook or predefined in connection of ssh_conn_id."
                )
                self.ssh_hook.remote_host = self.remote_host

            if not self.job_path:
                raise AirflowException("SSH command not specified. Aborting.")

            with self.ssh_hook.get_conn() as ssh_client:
                self.log.info("Running command: %s", self.job_path)

                command = '{} -file={}'.format(self.kitchen_path, self.job_path)
                if self.extra_params:
                    for param in self.extra_params:
                        command += ' -param:{}'.format(param)
                if self.extra_args:
                    for arg in self.extra_args:
                        command += ' {}'.format(arg)
                command += ' -level={}'.format(self.log_level)
                if self.log_path:
                    command += ' -logfile={}'.format(self.log_path)

                # set timeout taken as params
                stdin, stdout, stderr = ssh_client.exec_command(
                    command=command,
                    get_pty=self.get_pty,
                    timeout=self.timeout,
                    environment=self.environment,
                )
                # get channels
                channel = stdout.channel

                # closing stdin
                stdin.close()
                channel.shutdown_write()

                agg_stdout = b''
                agg_stderr = b''

                # capture any initial output in case channel is closed already
                stdout_buffer_length = len(stdout.channel.in_buffer)

                if stdout_buffer_length > 0:
                    agg_stdout += stdout.channel.recv(stdout_buffer_length)

                # read from both stdout and stderr
                while not channel.closed or channel.recv_ready() or channel.recv_stderr_ready():
                    readq, _, _ = select([channel], [], [], self.timeout)
                    for recv in readq:
                        if recv.recv_ready():
                            line = stdout.channel.recv(len(recv.in_buffer))
                            agg_stdout += line
                            self.log.info(line.decode('utf-8', 'replace').strip('\n'))
                        if recv.recv_stderr_ready():
                            line = stderr.channel.recv_stderr(len(recv.in_stderr_buffer))
                            agg_stderr += line
                            self.log.warning(line.decode('utf-8', 'replace').strip('\n'))
                    if (
                        stdout.channel.exit_status_ready()
                        and not stderr.channel.recv_stderr_ready()
                        and not stdout.channel.recv_ready()
                    ):
                        stdout.channel.shutdown_read()
                        stdout.channel.close()
                        break

                stdout.close()
                stderr.close()

                exit_status = stdout.channel.recv_exit_status()
                if exit_status == 0:
                    enable_pickling = conf.getboolean('core', 'enable_xcom_pickling')
                    if enable_pickling:
                        return agg_stdout
                    else:
                        return b64encode(agg_stdout).decode('utf-8')

                else:
                    error_msg = agg_stderr.decode('utf-8')
                    raise AirflowException(f"error running job: {self.job_path}, error: {error_msg}")

        except Exception as e:
            raise AirflowException(f"SSH operator error: {str(e)}")

        return True

    def tunnel(self) -> None:
        """Get ssh tunnel"""
        ssh_client = self.ssh_hook.get_conn()  # type: ignore[union-attr]
        ssh_client.get_transport()


class PentahoPanOperator(BaseOperator):
    """
    SSHOperator to execute commands on given remote host using the ssh_hook.

    :param ssh_hook: predefined ssh_hook to use for remote execution.
        Either `ssh_hook` or `ssh_conn_id` needs to be provided.
    :type ssh_hook: airflow.providers.ssh.hooks.ssh.SSHHook
    :param ssh_conn_id: connection id from airflow Connections.
        `ssh_conn_id` will be ignored if `ssh_hook` is provided.
    :type ssh_conn_id: str
    :param remote_host: remote host to connect (templated)
        Nullable. If provided, it will replace the `remote_host` which was
        defined in `ssh_hook` or predefined in the connection of `ssh_conn_id`.
    :type remote_host: str
    :param job_path: job path to execute on remote host.
    :type job: str
    :param log_level: level of kitchen log
    :type str
    :param log_path path to store log on remote host
    :type str
    :param kitchen_path: path to kitchen.sh on remote host
    :type str
    :param extra_params: extra params to use in job
    :type list
    :param extra_args: extra args to use in job
    :param timeout: timeout (in seconds) for executing the command. The default is 10 seconds.
    :type timeout: int
    :param environment: a dict of shell environment variables. Note that the
        server will reject them silently if `AcceptEnv` is not set in SSH config.
    :type environment: dict
    :param get_pty: request a pseudo-terminal from the server. Set to ``True``
        to have the remote process killed upon task timeout.
        The default is ``False`` but note that `get_pty` is forced to ``True``
        when the `command` starts with ``sudo``.
    :type get_pty: bool
    """

    template_fields = ('remote_host',)
    template_ext = ('.sh',)

    @apply_defaults
    def __init__(
        self,
        *,
        ssh_hook: Optional[SSHHook] = None,
        ssh_conn_id: Optional[str] = None,
        remote_host: Optional[str] = None,
        job_path: str = None,
        log_level: Optional[str] = 'Basic',
        log_path: Optional[str] = None,
        kitchen_path: Optional[str] = None,
        extra_params: Optional[list] = None,
        extra_args: Optional[list] = None,
        timeout: int = 10,
        environment: Optional[dict] = None,
        get_pty: bool = False,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.ssh_hook = ssh_hook
        self.ssh_conn_id = ssh_conn_id
        self.remote_host = remote_host
        self.job_path = job_path
        self.log_level = log_level
        self.log_path = log_path
        self.kitchen_path = kitchen_path
        self.extra_params = extra_params
        self.extra_args = extra_args
        self.timeout = timeout
        self.environment = environment
        self.get_pty = (self.job_path.startswith('sudo') or get_pty) if self.job_path else get_pty

    def execute(self, context) -> Union[bytes, str, bool]:
        try:
            if self.ssh_conn_id:
                if self.ssh_hook and isinstance(self.ssh_hook, SSHHook):
                    self.log.info("ssh_conn_id is ignored when ssh_hook is provided.")
                else:
                    self.log.info(
                        "ssh_hook is not provided or invalid. Trying ssh_conn_id to create SSHHook."
                    )
                    self.ssh_hook = SSHHook(ssh_conn_id=self.ssh_conn_id, timeout=self.timeout)

            if not self.ssh_hook:
                raise AirflowException("Cannot operate without ssh_hook or ssh_conn_id.")

            if self.remote_host is not None:
                self.log.info(
                    "remote_host is provided explicitly. "
                    "It will replace the remote_host which was defined "
                    "in ssh_hook or predefined in connection of ssh_conn_id."
                )
                self.ssh_hook.remote_host = self.remote_host

            if not self.job_path:
                raise AirflowException("SSH command not specified. Aborting.")

            with self.ssh_hook.get_conn() as ssh_client:
                self.log.info("Running command: %s", self.job_path)

                command = '{} -file={}'.format(self.kitchen_path, self.job_path)
                if self.extra_params:
                    for param in self.extra_params:
                        command += ' -param:{}'.format(param)
                if self.extra_args:
                    for arg in self.extra_args:
                        command += ' {}'.format(arg)
                if self.log_path:
                    command += ' -logfile={}'.format(self.log_path)
                command += ' -level={}'.format(self.log_level)

                # set timeout taken as params
                stdin, stdout, stderr = ssh_client.exec_command(
                    command=command,
                    get_pty=self.get_pty,
                    timeout=self.timeout,
                    environment=self.environment,
                )
                # get channels
                channel = stdout.channel

                # closing stdin
                stdin.close()
                channel.shutdown_write()

                agg_stdout = b''
                agg_stderr = b''

                # capture any initial output in case channel is closed already
                stdout_buffer_length = len(stdout.channel.in_buffer)

                if stdout_buffer_length > 0:
                    agg_stdout += stdout.channel.recv(stdout_buffer_length)

                # read from both stdout and stderr
                while not channel.closed or channel.recv_ready() or channel.recv_stderr_ready():
                    readq, _, _ = select([channel], [], [], self.timeout)
                    for recv in readq:
                        if recv.recv_ready():
                            line = stdout.channel.recv(len(recv.in_buffer))
                            agg_stdout += line
                            self.log.info(line.decode('utf-8', 'replace').strip('\n'))
                        if recv.recv_stderr_ready():
                            line = stderr.channel.recv_stderr(len(recv.in_stderr_buffer))
                            agg_stderr += line
                            self.log.warning(line.decode('utf-8', 'replace').strip('\n'))
                    if (
                        stdout.channel.exit_status_ready()
                        and not stderr.channel.recv_stderr_ready()
                        and not stdout.channel.recv_ready()
                    ):
                        stdout.channel.shutdown_read()
                        stdout.channel.close()
                        break

                stdout.close()
                stderr.close()

                exit_status = stdout.channel.recv_exit_status()
                if exit_status == 0:
                    enable_pickling = conf.getboolean('core', 'enable_xcom_pickling')
                    if enable_pickling:
                        return agg_stdout
                    else:
                        return b64encode(agg_stdout).decode('utf-8')

                else:
                    error_msg = agg_stderr.decode('utf-8')
                    raise AirflowException(f"error running job: {self.job_path}, error: {error_msg}")

        except Exception as e:
            raise AirflowException(f"SSH operator error: {str(e)}")

        return True

    def tunnel(self) -> None:
        """Get ssh tunnel"""
        ssh_client = self.ssh_hook.get_conn()  # type: ignore[union-attr]
        ssh_client.get_transport()

