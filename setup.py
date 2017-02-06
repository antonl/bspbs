from setuptools import setup, find_packages
setup(
    name="beanstalkpbs",
    version="0.1",
    packages=find_packages(),
    entry_points='''
        [console_scripts]
        bs-worker=beanstalkpbs.cli:start_worker
        bs-qsub=beanstalkpbs.cli:submit
        bs-showq=beanstalkpbs.cli:show_queues
    ''',
    requires=['beanstalkclient', 'click']
)
