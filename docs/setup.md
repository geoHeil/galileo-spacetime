---
title: Galileo Setup Guide
---

Setting Up Galileo
==================

The following tutorial will get a cluster of Galileo storage nodes up and running, ready to store files and service requests.

Prerequisites
-------------
* Java 7
* Bash
* [Galileo](http://galileo.cs.colostate.edu)


Passwordless SSH
----------------

Managing the cluster requires some form of passwordless login for any reasonably-sized installation; the cluster control scripts you find in the *bin* directory SSH to the machines and run a variety of commands to start, stop, or modify Galileo storage nodes' state.

In general, the best way to accomplish this is to generate a public and private SSH key pair, and then distribute the public key to machines in the cluster you wish to run storage nodes on.  I would recommend using a password on your private key combined with *ssh-agent* to make life easier, although if you're on a secure installation you can probably skip the private key password.  Either way, consult your operating system's documentation, or see [Using ssh-agent with ssh](http://mah.everybody.org/docs/ssh) by Mark A. Hershberger to get you up and running.


Environment Variables
---------------------

Galileo doesn't necessarily require any environment changes, and will try to store everything in its installation directory if it is writable.  If not, /tmp/$(username) is tried next.  This is fine for a single-node setup for testing purposes, but if you want to run a fully-fledged cluster then at a minimum you *must* set GALILEO_HOME to the location of your installation.  The remaining environment variables default to subdirectories under GALILEO_HOME.  Supported environment variables are:

* GALILEO_HOME - Galileo installation directory.
* GALILEO_CONF - Configuration directory; includes all the storage node configuration files.
* GALILEO_ROOT - Where file system root is stored.  File blocks, the journal, logs, etc. go here.

Setting these up depends on your shell.  In bash, they should be placed in ~/.bashrc.  In zsh, they go in ~/.zshenv.  Set them like so:

```bash
export GALILEO_HOME=/users/malensek/galileo
export GALILEO_CONF=/users/malensek/galileo/config
export GALILEO_ROOT=/srv/huge-disk/malensek/galileo
```

If you are using some variant of csh, then edit ~/.cshrc:

```tcsh
setenv GALILEO_HOME /users/malensek/galileo
setenv GALILEO_CONF /users/malensek/galileo/config
setenv GALILEO_ROOT /srv/huge-disk/malensek/galileo
```

You may also want to consider adding $GALILEO_HOME/bin to your PATH.

System Configuration
--------------------

Galileo is configured by editing files under $GALILEO_CONF.  Example files are stored in the "config" directory in your Galileo distribution.

### Groups

At a minimum, you must configure one or more Galileo _groups_, which define a set of machines within the cluster.  Group configuration files are stored under $GALILEO_CONF/network and end with a .group extension.  The files contain host names or IP addresses, one per line, and their storage node port number (if not set to the default port).  An example group, Test.group, is shown below:

```
lattice-0:5555
lattice-1
lattice-2
lattice-3
lattice-4
129.82.45.204:5055
```
