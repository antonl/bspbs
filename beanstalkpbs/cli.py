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
    conn.ignore('default')

    subq = []
    while True:
        j = conn.reserve(timeout=0.5)

        if not j:
            break

        job = Job.from_yaml(j.body)
        job.sync_with_job(j)
        subq.append(job)

    map(lambda job: conn.release(jid=job.id), subq)

    conn.watch('failed')
    conn.ignore('submitted')

    failedq = []
    while True:
        j = conn.reserve(timeout=0.5)

        if not j:
            break

        job = Job.from_yaml(j.body)
        job.sync_with_job(j)
        failedq.append(job)

    map(lambda job: conn.release(jid=job.id), failedq)

    conn.watch('completed')
    conn.ignore('failed')

    compq = []
    while True:
        j = conn.reserve(timeout=0.5)

        if not j:
            break

        job = Job.from_yaml(j.body)
        job.sync_with_job(j)
        compq.append(job)

    map(lambda job: conn.release(jid=job.id), compq)

    conn.close()

    # print queued jobs
    click.secho('Submitted jobs:')
    _render_job_table(subq)

    click.secho('Completed jobs:')
    _render_job_table(compq)

    click.secho('Failed jobs:')
    _render_job_table(failedq)

def _render_job_table(queue):
    click.secho('\t{id:6s} {command:64s} {ttr:15s}'.format(
        command='command', ttr='ttr', id='id'))
    click.secho('\t{id:^6s} {command:^64s} {ttr:15s}'.format(
        command='-'*64, ttr='-'*15, id='-'*6))

    for j in queue:
        fullcmd = ' '.join([j.cmd] + j.args)
        dt = str(datetime.timedelta(seconds=j.ttr))
        click.secho('\t{ranid:^6d} {command:64s} {ttr:15s}'.format(
            command=fullcmd,
                                                              ranid=j.ranid,
            ttr=dt))
        click.secho('\t{ranid:6s} wd: {cwd:61s} {ttr:15s}'.format(ranid='',
                                                                  ttr='',
                                                              cwd=j.cwd))

