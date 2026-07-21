type .\dataset\test.txt | python mapper.py
type .\dataset\test.txt | python mapper.py | sort
set NUM_NODES=4
set DAMPING=0.85
type input.txt | python mapper.py | sort | python reducer.py


& :: Get-Content .\dataset\test.txt | python mapper.py
& :: Get-Content .\dataset\test.txt | python mapper.py | Sort-Object
& :: $env:NUM_NODES=4
& :: $env:DAMPING=0.85
& :: Get-Content input.txt | python mapper.py | Sort-Object | python reducer.py
& :: Get-Content input.txt | python mapper.py | Sort-Object | python reducer.py > output1.txt