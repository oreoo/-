#!/usr/bin/env python3

import argparse
import os

import numpy as np


def main():
    argparser = argparse.ArgumentParser()
    argparser.add_argument('path', type=str)
    argparser.add_argument('size', type=int)

    args = argparser.parse_args()

    if not os.path.isdir(os.path.dirname(args.path)):
        os.makedirs(os.path.dirname(args.path))

    with open(args.path, 'w') as f:
        f.writelines(map(lambda x: f'{x}\n', np.random.normal(scale=1e-4, size=args.size)))


if __name__ == '__main__':
    main()
