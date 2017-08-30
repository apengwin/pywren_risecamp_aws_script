import os
import boto3
import shutil
from multiprocessing import Process
from pathlib import Path
import pywren


import numpy as np

ROOT_DIR = str(Path.home())

CONFIG_PATH = os.path.join(ROOT_DIR, ".pywren_config")
CRED_PATH = os.path.join(ROOT_DIR, ".aws/credentials")

ROOT_USER = 0
NUM_SUBUSERS = 20
CRED_DIR = os.path.abspath("pywren_creds")


def test_function(b):
#  x = np.random.normal(0, b, 1024)
#  A = np.random.normal(0, b, (1024, 1024))
#  return np.dot(A, x)
  return "hello world"


def verify_user(root, subuser):
  base = "user_%02d.subuser.user_%02d.deploy" % (root, subuser)

  if os.path.exists(CRED_PATH):
    os.unlink(CRED_PATH)

  if os.path.exists(CONFIG_PATH):
    os.unlink(CONFIG_PATH)

  cred_file = base + ".creds"
  os.symlink(os.path.join(CRED_DIR, cred_file), CRED_PATH)

  pywren_config = base + ".pywren_config.yaml"
  os.symlink(os.path.join(CRED_DIR, pywren_config), CONFIG_PATH)

  print(boto3.resource('iam').CurrentUser().arn)

  pwex = pywren.default_executor()
  futures = pwex.map(test_function, [0,1,2])

  results = [f.result(throw_except=False) for f in futures]
  if results != ["hello world", "hello world", "hello world"]:
    raise Exception("Problem executing lambda with account {} subuser {}".format(root, subuser) )
  print("success")

if __name__ == '__main__': 
  aws_dir = os.path.join(ROOT_DIR, ".aws")

  if os.path.exists(aws_dir):
    a = input("existing aws creds exit at ~/.aws. Overwrite?[yn]: ")
    if a[0] != 'y':
      exit()
    shutil.rmtree(aws_dir)
  os.mkdir(aws_dir)

  with open(os.path.join(aws_dir, "config"), 'w') as f:
    f.write("[default]\nregion = us-west-2")

  for user in range(NUM_SUBUSERS):
    # We need to fork a new process so boto3 loads the new credentials.
    p = Process(target=verify_user, args=(ROOT_USER, user))
    p.start()
    p.join()

  print()
  print("All done")
