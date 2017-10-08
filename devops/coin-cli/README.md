# Numismatic coin-cli docker environment
Build and run a numismatic coin-cli from an already configured environment

**Dependencies:**
 - Packer
 ```
 curl -O https://releases.hashicorp.com/packer/1.1.0/packer_1.1.0_linux_amd64.zip
 unzip packer_1.1.0_linux_amd64.zip -d /usr/local/bin
 ```
 - Docker (see https://docs.docker.com/engine/installation/)

**Build:**
  - `cd ./devops/coin-cli/packer/builders/docker`
  - `. ./coin-cli.build`

**Run:**
  - `cd ./devops/coin-cli/packer/builders/docker`
  - `. ./coin-cli.run`
  - Each of the following commands, when executed in the same shell as the
    command above, mounts the current working directory into a Docker container
    and runs the following process:
    - `dcoin`: numismatic coin-cli with the numismatic environment configured
    - `coinbash`: A Bash shell with the numismatic environment configured
    - `coinpy`: A Python shell with the numismatic environment configured

**Tips:**
  - In order to have the commands in the section above persistently available:
      - `cat ./coin-cli.run >> ~/.bash_aliases`
      - or `cp ./coin-cli.run ~/.oh-my-zsh/custom/coin-cli.zsh`
      (if you use oh-my-zsh)
