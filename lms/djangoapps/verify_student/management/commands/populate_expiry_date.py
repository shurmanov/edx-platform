"""
Django admin command to populate expiry_date for approved verifications in SoftwareSecurePhotoVerification
"""
import logging
import time
from datetime import timedelta

from django.conf import settings
from django.core.management.base import BaseCommand
from util.query import use_read_replica_if_available

from lms.djangoapps.verify_student.models import SoftwareSecurePhotoVerification

logger = logging.getLogger(__name__)


class Command(BaseCommand):
    """
    This command sets the expiry_date for users for which the verification is approved

    The task is performed in batches with maximum number of rows to process given in argument `batch_size`
    and a sleep time between each batch given by `sleep_time`

    Default values:
        `batch_size` = 1000 rows
        `sleep_time` = 10 seconds

    Example usage:
        $ ./manage.py lms populate_expiry_date --batch_size=1000 --sleep_time=5
    OR
        $ ./manage.py lms populate_expiry_date
    """
    help = 'Populate expiry_date for approved verifications'

    def add_arguments(self, parser):
        parser.add_argument(
            '--batch_size',
            action='store',
            dest='batch_size',
            type=int,
            default=1000,
            help='Maximum number of database rows to process. '
                 'This helps avoid locking the database while updating large amount of data.')
        parser.add_argument(
            '--sleep_time',
            action='store',
            dest='sleep_time',
            type=int,
            default=10,
            help='Sleep time in seconds between update of batches')

    def handle(self, *args, **options):
        """
        Handler for the command

        It filters approved Software Secure Photo Verification and then for each distinct user it finds the most
        recent approved verification and set its expiry_date
        """
        batch_size = options['batch_size']
        sleep_time = options['sleep_time']

        query = SoftwareSecurePhotoVerification.objects.filter(status='approved').order_by('user_id')
        sspv = use_read_replica_if_available(query)

        if not sspv.count():
            logger.info("No approved entries found in SoftwareSecurePhotoVerification")
            return

        users = sspv.values('user_id').distinct()
        updated_verification_count = 0

        for user in users:
            recent_verification = self.find_recent_verification(sspv, user['user_id'])
            if not recent_verification.expiry_date:
                recent_verification_qs = SoftwareSecurePhotoVerification.objects.filter(pk=recent_verification.pk)
                recent_verification_qs.update(expiry_date=recent_verification.updated_at + timedelta(
                    days=settings.VERIFY_STUDENT["DAYS_GOOD_FOR"]))
                updated_verification_count += 1

            if updated_verification_count == batch_size:
                updated_verification_count = 0
                time.sleep(sleep_time)

    def find_recent_verification(self, model, user_id):
        """
        Returns the most recent approved verification for a user
        """
        return model.filter(user_id=user_id).latest('updated_at')
