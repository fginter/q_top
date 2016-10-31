import subprocess
import datetime

def user_jobs(user):
    proc = subprocess.Popen(["squeue","-u",user],stdout=subprocess.PIPE,stderr=subprocess.PIPE)
    outs, errs = proc.communicate()
    if proc.returncode:
        raise ValueError("Squeue failed")
    return outs.decode("ASCII").count("\n")

def top_up(count,args):
    with open(args.commands) as f:
        commands=f.read().splitlines()

    to_run=commands[:count]
    for_later=commands[count:]
    with open(args.finished,"a") as fin, open(args.commands,"w") as later, open(args.log,"a") as log:
        print("#Submitting {} jobs at {}".format(len(to_run),datetime.datetime.now().isoformat()),file=fin)
        for j in to_run:
            proc=subprocess.Popen(j,stdout=subprocess.PIPE,stderr=subprocess.PIPE,shell=True)
            o,e=proc.communicate()
            if proc.returncode==0:
                print(j,file=fin)
            else:
                print(j,file=later)
                print("FAIL with code {} at {}".format(proc.returncode,datetime.datetime.now().isoformat()),file=log)
                print(j,file=log)
                print(o.decode("ASCII"),file=log)
                print(e.decode("ASCII"),file=log)
        print("\n".join(for_later),file=later)

if __name__=="__main__":
    import argparse
    parser = argparse.ArgumentParser(description='Taito queue topper')
    parser.add_argument('-N', '--N', metavar='N', type=int, nargs=1,help='How many jobs in the queue do we want?')
    parser.add_argument('--commands', dest='commands', action='store',help='One bsub command per line')
    parser.add_argument('--finished', dest='finished', action='store',help='Will write the finished commands to here')
    parser.add_argument('--log', dest='log', action='store',help='Log for failed commands')
    parser.add_argument('--user', dest='user', action='store',help='Which user to check?')

    args = parser.parse_args()

    jobs_existing=user_jobs(args.user)
    if jobs_existing<args.N[0]: #Can top up
        top_up(args.N[0]-jobs_existing,args)

