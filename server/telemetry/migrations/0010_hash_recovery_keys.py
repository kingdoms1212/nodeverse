"""Hash plaintext game account recovery keys (SV-2). Irreversible by design."""
from django.db import migrations


def hash_keys(apps, schema_editor):
    from telemetry.game_auth_api import RECOVERY_PREFIX, recovery_digest
    GameAccount = apps.get_model('telemetry', 'GameAccount')
    for account in GameAccount.objects.exclude(recovery_key='').exclude(recovery_key__startswith=RECOVERY_PREFIX).iterator():
        account.recovery_key = recovery_digest(account.recovery_key)
        account.save(update_fields=['recovery_key'])


class Migration(migrations.Migration):
    dependencies = [('telemetry', '0009_alter_gameloginrecord_kind_and_more')]
    operations = [migrations.RunPython(hash_keys, migrations.RunPython.noop)]
