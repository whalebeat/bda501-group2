@echo off

set NUM_NODES=4
set DAMPING=0.85

copy .\dataset\test.txt iter0.txt >nul

for /L %%i in (1,1,10) do (
    echo Running iteration %%i

    type iter%%i-1.txt ^
    | python mapper.py ^
    | sort ^
    | python reducer.py > .\output\iter%%i.txt
)

echo Done