<p align="center">
Large-Scale Distributed Systems</br>
Simple Group Messenger </br>

CSE 586 - Spring 2019
</p>

---------------


Goal
------
Design a group messenger that can send message to multiple AVDs and store them in a permanent key-value storage



Testing
----------


The  [**Grader**] test our implementation rigorously using multiple threads. Refer [**Project Specifications**](https://docs.google.com/document/d/1nWaDn2joq-pFmePUjv_hMjO_NrvnmqVmIKGbjET2p5Q/edit#) for details: - 


Running the Grader/Testing Program
----------------------------------------------------
> 1. Load the Project in Android Studio and create the [**apk file**](https://developer.android.com/studio/run/index.html).
> 2. Download  the [**Testing Program**] for your platform.
> 3. Please also make sure that you have installed the app on all the AVDs.
> 4. Before you run the program, please make sure that you are **running five AVDs**. The below command will do it: -
	- **python [run_avd.py] 5**
> 5. Also make sure that the **Emulator Networking** setup is done. The below command will do it: -
	- **python [set_redir.py] 10000**
> 6.  Run the grader: -
	- $ chmod +x ***< grader executable>***
    - $ ./*< grader executable>* ***apk file path***



Credits
-------

I acknowledge and grateful to [**Professor Steve ko**](https://nsr.cse.buffalo.edu/?page_id=272) for his continuous support throughout the Course ([**CSE 586**](http://www.cse.buffalo.edu/~stevko/courses/cse486/spring16/)) that helped me learn the skills of Large Scale Distributed Systems and develop a **simple group messenger**.

