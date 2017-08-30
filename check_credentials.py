import os
import boto3
import shutil
from multiprocessing import Process
from pathlib import Path
import pywren


import numpy as np

ROOT_DIR = os.getcwd()

CONFIG_PATH = os.path.join(ROOT_DIR, ".pywren_config")
AWS_CREDS = os.path.join(ROOT_DIR, ".aws/credentials")

CRED_DIR = os.path.abspath("pywren_creds")

ROOT_USER = 0
NUM_SUBUSERS = 20

def test_function(b):
#  x = np.random.normal(0, b, 1024)
#  A = np.random.normal(0, b, (1024, 1024))
#  return np.dot(A, x)
  return "hello world"


def verify_user(root, subuser):
  base = "user_%02d.subuser.user_%02d.deploy" % (root, subuser)

  if os.path.exists(AWS_CREDS):
    os.unlink(AWS_CREDS)

  if os.path.exists(CONFIG_PATH):
    os.unlink(CONFIG_PATH)

  cred_file = base + ".creds"
  os.symlink(os.path.join(CRED_DIR, cred_file), AWS_CREDS)

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
  print(aws_dir)
  if os.path.exists(aws_dir):
    shutil.rmtree(aws_dir)
  os.makedirs(aws_dir)

  with open(os.path.join(aws_dir, "config"), 'w') as f:
    f.write("[default]\nregion = us-west-2")

  for user in range(NUM_SUBUSERS):
    # We need to fork a new process so boto3 loads the new credentials.
    os.environ["AWS_SHARED_CREDENTIALS_FILE"] = AWS_CREDS
    p = Process(target=verify_user, args=(ROOT_USER, user))
    p.start()
    p.join()

  shutil.rmtree(aws_dir)
  os.unlink(CONFIG_PATH)
  print()
  print("All done")
