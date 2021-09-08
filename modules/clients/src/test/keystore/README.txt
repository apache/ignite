This directory contains certification authorities, trust stores and keys, that are used in tests.

In order to generate CAs, run the generate-ca.sh script.
It will create all needed CAs from scratch and all needed trust-stores.
In order for it to work, the ca directory should be removed.

To generate keys, run the generate-keys.sh script.
In order to create new keys, you can comment out calls to createStore, add new ones and run the script.

If keys are expired and need to be generated again, the easiest way is to generate CAs from scratch and replace all
keys with the new ones.
