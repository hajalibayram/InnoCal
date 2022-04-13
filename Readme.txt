Go to InnoCal folder.
cmd ::: cd \
cmd ::: cd InnoCal

Run Setup.py only for the first time
cmd ::: C:\Users\stage.bayramov\AppData\Local\Programs\Python\Python310\python.exe C:\InnoCal\Setup.py

Run InnoCal.py for every time to sync
cmd ::: C:\Users\stage.bayramov\AppData\Local\Programs\Python\Python310\python.exe C:\InnoCal\InnoCal.py



Possible errors:

Pip installation if it does not exist:

cmd ::: curl https://bootstrap.pypa.io/get-pip.py -o get-pip.py
cmd ::: C:\Users\stage.bayramov\AppData\Local\Programs\Python\Python310\python.exe get-pip.py

copy installed pip from "C:\Users\stage.bayramov\AppData\Local\Programs\Python\Python310\Scripts" to pip by command or manually

cmd ::: copy C:\Users\stage.bayramov\AppData\Local\Programs\Python\Python310\Scripts\pip C:\Users\stage.bayramov\AppData\Local\Programs\Python\Python310