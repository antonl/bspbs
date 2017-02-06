import click
import beanstalk
import os
import datetime
from .core import Worker, Job

@click.command()
@click.option('--host', default='127.0.0.1')
@click.option('--port', default=11300)
def start_worker(host, port):
    print('Connecting to queue at {host}:{port}'.format(host=host, port=port))
    try:
        w = Worker(host, port)
        w.handle()
    except RuntimeError as e:
        print(e)
    print('Shut down worker')

@click.command()
@click.option('--host', default='127.0.0.1')
@click.option('--port', default=11300)
@click.option('--ttr', default=60.)
@click.argument('cmd', nargs=1)
@click.argument('args', nargs=-1)
def submit(host, port, cmd, args, ttr):
    env = os.environ.copy()
    port=int(port)
    job = Job()
    job.cmd = cmd
    job.args = args
    job.env = env
    job.cwd = os.getcwd()
    job.ttr = float(ttr)
    #print(job.to_yaml())
    conn = beanstalk.Connection(host, port, parse_yaml=True)
    conn.use('submitted')
    jid = conn.put(job.to_yaml(), ttr=ttr)
    print('submitted job {jobid}'.format(jobid=jid))
    conn.close()

@click.command()
@click.option('--host', default='127.0.0.1')
@click.option('--port', default=11300)
def show_queues(host, port):
    conn = beanstalk.Connection(host, port, parse_yaml=True)
    conn.watch('submitted')
    conn.use('submitted')

    stats = conn.stats_tube('submitted')
    #print(stats)
    maxj = max(stats['total-jobs'], stats['current-jobs-buried'])

    subq = []
    for i in range(1, maxj+1):
        j = conn.peek(i)
        job = Job.from_yaml(j.body)
        job.sync_with_job(j)
        subq.append(job)

    conn.close()

    # print queued jobs
    click.secho('Reserved jobs:')
    _render_job_table(filter(lambda x: x.state == 'reserved', subq))

    click.secho('Ready jobs:')
    _render_job_table(filter(lambda x: x.state == 'ready', subq))

    click.secho('Completed jobs:')
    _render_job_table(filter(lambda x: x.state == 'buried', subq))

def _render_job_table(queue):
    click.secho('\t{id:6s} {command:64s} {ttr:15s} {time_left:15s}'.format(
        command='command', ttr='ttr', id='id', time_left='time left'))
    click.secho('\t{id:^6s} {command:^64s} {ttr:15s}'.format(
        command='-'*64, ttr='-'*15, id='-'*6, time_left='-'*15))

    for j in queue:
        fullcmd = ' '.join([j.cmd] + j.args)
        dt = str(datetime.timedelta(seconds=j.ttr))
        dl = str(datetime.timedelta(seconds=j.time_left))
        click.secho('\t{id:^6d} {command:64s} {ttr:15s} {time_left:15s}'.format(
            command=fullcmd, id=j.id, ttr=dt, time_left=dl))
        click.secho('\t{id:6s} wd: {cwd:61s} {ttr:15s} {time_left:15s}'.format(
            id='', ttr='', cwd=j.cwd, time_left=''))

