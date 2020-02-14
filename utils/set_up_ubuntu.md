# Instructions to set up a python environment on Ubuntu 18.04

First, update and upgrade apt-get registry:
```bash
sudo apt-get update
sudo apt-get upgrade
```

If you are using a remote machine with SSH, it is a good idea to install tmux and start it:
```bash
sudo apt-get install tmux
tmux
```
> Hint: SSH connections may time out if a program takes too long to run without printing
>anything; tmux sessions ensure that your program will not be aborted if the connection
>is reset.

Install pyenv:
```bash
# Install prerequisites
sudo apt-get install -y make build-essential libssl-dev zlib1g-dev libbz2-dev \
libreadline-dev libsqlite3-dev wget curl llvm libncurses5-dev libncursesw5-dev \
xz-utils tk-dev libffi-dev liblzma-dev python-openssl git

# Install and configure pyenv; then restart shell
curl https://pyenv.run | bash
echo "export PATH=\"$HOME/.pyenv/bin:\$PATH\"" >> ~/.bashrc
echo "eval \"\$(pyenv init -)\"" >> ~/.bashrc
echo "eval \"\$(pyenv virtualenv-init -)\"" >> ~/.bashrc
exec $SHELL
```

Update pyenv, install python and set it to global:
```bash
pyenv update
pyenv install 3.7.6
pyenv global 3.7.6
```

If using psycopg2, install `libpq-dev`
```bash
sudo apt-get install libpq-dev
```
