Build Instructions
==================

To compile the project, place all submitted .java files in the same directory and run: javac *.java


Working Functionality
=====================

Describe what you think should work.

----------------------------------
-------------------------------
------------------------------

IN2011 Computer Networks - Coursework 1
Submission by:
DOMINYKAS_MILKINTAS
Student ID: YOUR_STUDENT_ID_NUMBER
Email: YOUR_EMAIL_HERE


Build Instructions
------------------
To compile the project, place all submitted .java files in the same directory and run:

javac *.java


Run Instructions
----------------
To run the local test:

java LocalTest

To run the local test with more nodes:

java LocalTest 5
java LocalTest 10

To run the Azure lab smoke test:

java AzureLabTest YOUR_EMAIL_HERE 10.x.x.x 20110

Replace YOUR_EMAIL_HERE with the City email address used for the node name.
Replace 10.x.x.x with the Azure lab machine IP address.
The port should normally be in the recommended CRN range, for example 20110.


Completed Functionality
-----------------------
The implementation includes:

- SHA-256 hashID generation.
- Hash distance calculation using matching leading bits.
- CRN string encoding and decoding.
- Name messages: G/H.
- Nearest messages: N/O.
- Key existence messages: E/F.
- Read messages: R/S.
- Write messages: W/X.
- UDP request/response handling with transaction IDs.
- Timeout and retry behaviour.
- Relay message handling: V.
- Compare-and-swap messages: C/D.
- Basic address storage rules.
- Basic data rebalancing behaviour.
- Handling of malformed or unknown messages without crashing.


Testing Performed
-----------------
The implementation was tested using:

javac *.java
java LocalTest
java LocalTest 5
java LocalTest 10

The implementation was also tested with additional temporary tests for:
- hash output length,
- distance calculation,
- CRN string encoding/decoding,
- name request/response,
- nearest request/response,
- key existence,
- read,
- write,
- active node checking,
- relay,
- compare-and-swap,
- malformed messages.


Known Limitations
-----------------
This implementation attempts to follow the CRN RFC, but there may be limitations in complex network conditions.

Known limitations:
- Rebalancing is best-effort.
- Address stability preference is simple rather than advanced.
- Very complex relay paths may not have been tested extensively.
- The Azure lab network result may depend on available peer nodes and network conditions.