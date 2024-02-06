#!/usr/bin/env python
import argparse
import math

def linspace(a, b, n):
    '''Numpyless linear spacing function.
    '''
    if n < 2:
        return a
    diff = (float(b) - a)/(n - 1)
    return [diff * i + a for i in range(n)]


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('ampx0', type=float)
    parser.add_argument('ampx1', type=float)
    parser.add_argument('ampy0', type=float)
    parser.add_argument('ampy1', type=float)
    parser.add_argument('number', type=int)
    args = parser.parse_args()

    num_part_div = int(math.sqrt(args.number))
    amp_x = linspace(args.ampx0, args.ampx1, num_part_div+1)
    amp_y = linspace(args.ampy0, args.ampy1, num_part_div+1)
    offset = (args.ampx1 - args.ampx0) / (2.*float(num_part_div))
    amp_x = [x + offset for x in amp_x[:-1]]
    amp_y = [y + offset for y in amp_y[:-1]]
    with open('input_dist.txt', 'w') as fp:
        for i, ampx in enumerate(amp_x):
            for j, ampy in enumerate(amp_y):
                rx = math.sqrt(ampx)
                ry = math.sqrt(ampy)

                # particle IDs
                pid = 2*(i*len(amp_y) + j)
                pid_twin = pid + 1
                fp.write('{:d} {:.10f} {:.10f}\n'.format(
                    pid, rx, ry))
                fp.write('{:d} {:.10f} {:.10f}\n'.format(
                    pid_twin, rx+1e-6, ry+1e-6))
