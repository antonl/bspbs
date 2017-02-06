import beanstalk
import straitlets
import subprocess
import time
import os
import pathlib

class Job(straitlets.Serializable):
    cmd = straitlets.Unicode(help='command runnable by a shell', default_value='')

    args = straitlets.List(trait=straitlets.Unicode,
                           help='list of arguments to add to the command',
                           default_value=list())

    id = straitlets.Integer(help='job id on queue', default_value=-1)

    env = straitlets.Dict(help='environment variables to add to the run',
                          default_value={})
    cwd = straitlets.Unicode(help='current working directory',
                             default_value='.')
    ttr = straitlets.Float(help='current time-to-run of task',
                           default_value=120.)
    time_left = straitlets.Float(help='current time left for task',
                                default_value=-1.)
    state = straitlets.Enum(['ready', 'reserved', 'delayed', 'buried',
                             'unsubmitted'], default_value='unsubmitted')


    def sync_with_job(self, job_object):
        self.id = job_object.jid
        self.conn = job_object.conn
        #print(job_object.stats())
        self.state = job_object.stats()['state']
        self.time_left = job_object.stats()['time-left']

    def start(self):
        env = os.environ.copy()
        env.update(self.env)
        lst = [self.cmd] + self.args
        cwd = pathlib.Path(self.cwd).absolute()

        stdout = cwd / 'o{jobid:04d}.out'.format(jobid=self.id)
        stderr = cwd / 'e{jobid:04d}.out'.format(jobid=self.id)

        with stdout.open('w') as stdo, stderr.open('w') as stde:
            self.proc = subprocess.Popen(lst, stdout=stdo, stderr=stde,
                                         env=env, cwd=str(cwd))

class Worker:
    def __init__(self, host, port):
        try:
            self._conn = beanstalk.Connection(host=host, port=port,
                                                parse_yaml=True)
            self._conn.watch('submitted')
        except beanstalk.exceptions.SocketError:
            raise RuntimeError('Could not connect to server, connection '
                              'refused.')


    def handle_job(self):
        try:
            self._conn.watch('submitted')

            j = self._conn.reserve(timeout=0.5)
            if j is None:
                # timed out
                return

            job = Job.from_yaml(j.body)
            job.sync_with_job(j)
        except KeyboardInterrupt:
            print('Got ^C punt, quitting')
            raise KeyboardInterrupt

        error = None

        try:
            print('Executing job {jobid}'.format(jobid=job.id))
            job.start()
            while True:
                stats = self._conn.stats_job(job.id)

                if stats['time-left'] < 1:
                    print('Job out of time, terminating...')
                    error = 'failed'
                    job.proc.kill()
                    time.sleep(1)

                if job.proc.poll() is not None:
                    # proc terminated, break out
                    break

                time.sleep(0.5)
                #print('tick')
            print('Finished job.')
        except KeyboardInterrupt:
            print('Got ^C punt, quitting')
            error = 'failed'
        finally:
            job.proc.kill()
            self._conn.bury(job.id)

    def handle(self):
        while True:
            try:
                self.handle_job()
                time.sleep(0.5)
            except KeyboardInterrupt:
                print('Received ^C, exiting')
                self._conn.close()
                break
            except beanstalk.exceptions.SocketError:
                print('Lost connection to server, exiting')
                self._conn.close()
                break

