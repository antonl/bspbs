# Beanstalk PBS motivation

I want a lightweight batch system for *nix machines so that I could queue up
long-running simulations and execute them one after the other. The idea is to
simply run the queued commands using a worker process using subprocess. I
don't do anything special to take care of the running environment.

## Queues

There are a few queues on the server:

- ```ready```: new jobs are added here
- ```completed```: jobs that have been submitted successfully. Jobs are added
 to this queue after a worker reserves a job from ```ready```, and then it is
 moved here when it completes
- ```failed```: jobs that failed are moved here.