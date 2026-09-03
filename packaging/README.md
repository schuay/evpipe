# Running evpipe as a service

`evpipe.service` is the shell pipeline from the top-level README, kept
alive by systemd. It runs on host A -- the machine where the events
should land -- opens the ssh connection to host B, and pipes the
sender's output into a local `evpipe-recv`:

```
ssh $EVPIPE_SSH_OPTS $EVPIPE_SSH_TARGET "$EVPIPE_SEND_CMD $EVPIPE_SEND_ARGS" | $EVPIPE_RECV_CMD $EVPIPE_RECV_ARGS
```

Everything on that line comes from `~/.config/evpipe/evpipe.conf`; the
unit itself never needs editing.

## Install (user service)

```
mkdir -p ~/.config/systemd/user ~/.config/evpipe
cp packaging/evpipe.service ~/.config/systemd/user/
cp packaging/evpipe.conf.example ~/.config/evpipe/evpipe.conf
$EDITOR ~/.config/evpipe/evpipe.conf          # at minimum EVPIPE_SSH_TARGET + EVPIPE_SEND_ARGS
systemctl --user daemon-reload
systemctl --user enable --now evpipe.service
journalctl --user -u evpipe -f
```

A user service is the direct translation of running it in your own
shell: same user, same `~/.ssh`, same access to `/dev/uinput`. To keep
it running when you are not logged in, `loginctl enable-linger $USER`.

## Prerequisites

* `/dev/uinput` readable+writable by you on A, and the source
  `/dev/input/event*` nodes readable by the remote user on B (usually
  the `input` group on both). If the manual pipeline works today, this
  is already done.
* Passwordless ssh from A to B. The unit sets `BatchMode=yes`, so ssh
  will not prompt -- a user service has no `ssh-agent` in its
  environment, so use a passphraseless key (or an agent socket you
  point at explicitly), and make sure B's host key is already in
  `known_hosts`. A first connection that wants to confirm a fingerprint
  fails instead of hanging.

## Restart behaviour

`Restart=always` with no start rate limit. A dropped link is the normal
case here -- B reboots, wifi drops, the lid closes -- and `evpipe-recv`
exits 0 on EOF, so `on-failure` would never fire. The service retries
every 5s until B answers again. Stopping the service kills ssh, which
makes sshd send `SIGHUP` to the sender on B, which releases its grabs.

The cost is that a genuinely broken config (wrong host, bad path) also
retries forever rather than failing loudly. `journalctl --user -u
evpipe` shows ssh's and the receiver's stderr.

## System service instead

If you want it up before anyone logs in and without lingering, install
to `/etc/systemd/system/`, add a `User=`/`Group=` for an account that
can open `/dev/uinput`, and point `EnvironmentFile=` at an absolute
path (`%h` still resolves to that user's home, so the default works if
they have one). `WantedBy=default.target` becomes
`WantedBy=multi-user.target`. That account needs its own ssh key for B.
