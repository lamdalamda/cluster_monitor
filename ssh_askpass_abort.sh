#!/bin/sh
# 给 app.py 的 ssh 用的 SSH_ASKPASS：服务器一旦要求输入（2FA 验证码 / 密码 / 确认 host key），
# 不回答任何东西，直接把 ssh 杀掉，并把提示语告诉 app.py。
# 这样不会因为发了空的验证码而累积失败次数、被锁账号。
echo "@@NEED_INPUT $1" >&2
kill -TERM "$PPID"
exit 1
