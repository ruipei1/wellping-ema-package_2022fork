#!/bin/bash

rm -r build/
sleep 1

rm -r dist/
sleep 1

rm -r wellping_ema_parser.egg-info
sleep 1

python3 setup.py sdist bdist_wheel
sleep 3
python3 -m twine upload --verbose dist/*