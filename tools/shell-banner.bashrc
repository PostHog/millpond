# DuckLake target banner. Appended to /etc/bash.bashrc at image build
# (Debian bash reads it for every interactive non-login shell, which is
# what `kubectl exec -it ... -- bash` spawns; the runtime rootfs is
# read-only and exec'd shells never touch PAM/motd, so this is the one
# hook that reliably fires).
#
# Prints which DuckLake catalog this container is wired to, derived from
# the standard DUCKLAKE_* env, so an operator can't mistake one catalog's
# maintenance shell for another's before running expire/compact/delete.
#
# MILLPOND_SHELL_LABEL optionally overrides the derived label with a
# deployment-specific name (set it in your pod spec).
if [ -n "$DUCKLAKE_RDS_HOST" ] && [ -z "$MILLPOND_BANNER_SHOWN" ]; then
    export MILLPOND_BANNER_SHOWN=1
    _dl_label="${MILLPOND_SHELL_LABEL:-${DUCKLAKE_RDS_DATABASE:-${DUCKLAKE_RDS_HOST%%.*}}}"
    printf '\n\033[1;33m=== DUCKLAKE MAINTENANCE SHELL ===\033[0m\n'
    printf 'Catalog: %s\n' "$_dl_label"
    printf 'RDS:     %s / %s\n' "$DUCKLAKE_RDS_HOST" "${DUCKLAKE_RDS_DATABASE:-?}"
    printf 'Data:    %s\n' "${DUCKLAKE_DATA_PATH:-?}"
    printf '\033[1;31mCheck the target above before any expire/compact/delete.\033[0m\n\n'
    PS1="\[\033[1;33m\]${_dl_label}\[\033[0m\]|\w\$ "
    unset _dl_label
fi
