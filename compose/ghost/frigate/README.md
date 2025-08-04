# Frigate

## Coral TPU

The Coral device needs to be setup on any new system

[link](https://coral.ai/docs/m2/get-started/#2a-on-linux)

```
echo "deb https://packages.cloud.google.com/apt coral-edgetpu-stable main" | sudo tee /etc/apt/sources.list.d/coral-edgetpu.list

curl https://packages.cloud.google.com/apt/doc/apt-key.gpg | sudo apt-key add -

sudo apt-get update
```

The below command failed for me on Ubuntu 24 with kernel 6.8.x

```
sudo apt-get install gasket-dkms libedgetpu1-std
```

So I went [here](https://github.com/google-coral/edgetpu/issues/808#issuecomment-1909019568)

```
sudo sh -c "echo 'SUBSYSTEM==\"apex\", MODE=\"0660\", GROUP=\"apex\"' >> /etc/udev/rules.d/65-apex.rules"
sudo groupadd apex

sudo adduser $USER apex

```

### Then Reboot!

```
lspci -nn | grep 089a
```
