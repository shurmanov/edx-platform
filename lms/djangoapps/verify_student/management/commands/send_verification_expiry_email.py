"""
Django admin command to send verification expiry email to learners
"""
import logging
import time
from datetime import datetime, timedelta

from django.conf import settings
from django.contrib.auth.models import User
from django.contrib.sites.models import Site
from django.core.management.base import BaseCommand
from django.urls import reverse
from edx_ace import ace
from edx_ace.recipient import Recipient
from pytz import UTC
from util.query import use_read_replica_if_available
from verify_student.message_types import VerificationExpiry

from lms.djangoapps.verify_student.models import SoftwareSecurePhotoVerification
from openedx.core.djangoapps.ace_common.template_context import get_base_template_context
from openedx.core.djangoapps.lang_pref import LANGUAGE_KEY
from openedx.core.djangoapps.user_api.preferences.api import get_user_preference
from openedx.core.lib.celery.task_utils import emulate_http_request

logger = logging.getLogger(__name__)


class Command(BaseCommand):
    """
    This command sends email to learners for which the Software Secure Photo Verification has expired

    The expiry email is sent when the date represented by SoftwareSecurePhotoVerification's field `expiry_date`
    lies within the date range provided by command arguments. If the email is already sent indicated by field
    `expiry_email_date` then resend after the specified number of days given as command argument `--resend_days`

    The range to filter expired verification is selected based on --days-range. This represents the number of days
    before now and gives us start_date of the range
         Range:       start_date to today

    The task is performed in batches with maximum number of users to send email given in `batch_size` and the
    delay between batches is indicated by `sleep_time`.For each batch a celery task is initiated that sends the email

    Example usage:
        $ ./manage.py lms send_verification_expiry_email --resend-days=30 --batch-size=2000 --sleep-time=5
    OR
        $ ./manage.py lms send_verification_expiry_email

    To run the command without sending emails:
        $ ./manage.py lms send_verification_expiry_email --dry-run
    """
    help = 'Send email to users for which Software Secure Photo Verification has expired'

    def add_arguments(self, parser):
        parser.add_argument(
            '-d', '--resend-days',
            type=int,
            default=15,
            help='Desired days after which the email will be resent to learners with expired verification'
        )
        parser.add_argument(
            '--batch-size',
            type=int,
            default=1000,
            help='Maximum number of users to send email in one celery task')
        parser.add_argument(
            '--sleep-time',
            type=int,
            default=10,
            help='Sleep time in seconds between update of batches')
        parser.add_argument(
            '--days-range',
            type=int,
            default=365,
            help="The number of days before now to check expired verification")
        parser.add_argument(
            '--dry-run',
            action='store_true',
            help='Gives the ranges of user_id for which email will be sent')

    def handle(self, *args, **options):
        """
        Handler for the command

        It creates batches of expired Software Secure Photo Verification against which email was not already sent
        and initiates a celery task against it
        """
        resend_days = options['resend_days']
        batch_size = options['batch_size']
        sleep_time = options['sleep_time']
        days = options['days_range']

        start_date = datetime.now(UTC) - timedelta(days=days)
        query = SoftwareSecurePhotoVerification.objects.filter(status='approved',
                                                               expiry_date__lt=datetime.now(UTC),
                                                               expiry_date__gte=start_date
                                                               )
        sspv = use_read_replica_if_available(query)

        total_verification = sspv.count()
        if not total_verification:
            logger.info("No approved expired entries found in SoftwareSecurePhotoVerification for the "
                        "date range {} - {}".format(start_date.date(), datetime.now(UTC).date()))
            return

        if total_verification < batch_size:
            batch_size = total_verification

        # This number does not necessarily represent the actual number of learners to which email was sent
        # The email will be sent to equal or fewer learners. It will be fewer than the total_verification
        # in the case where email was already sent
        logger.info("For the date range {} - {}, total Software Secure Photo verification filtered are {}"
                    .format(start_date.date(), datetime.now(UTC).date(), total_verification))

        # If email was sent and user did not re-verify then this date will be used as the criteria for resending email
        date_resend_days_ago = datetime.now(UTC) - timedelta(days=resend_days)
        batch_verifications = []

        for verification in sspv:
            if not verification.expiry_email_date or verification.expiry_email_date < date_resend_days_ago:
                batch_verifications.append(verification)

                if len(batch_verifications) == batch_size or total_verification < batch_size:
                    if not options['dry_run']:
                        send_verification_expiry_email(batch_verifications)
                        time.sleep(sleep_time)
                    else:
                        logger.info(
                            "This was a dry run, no email was sent. For the actual run email would have been sent "
                            "for expired verification within the range: user id {} - user id {}".format(
                                batch_verifications[0].user_id, batch_verifications[-1].user_id))
                    total_verification = total_verification - batch_size
                    batch_verifications = []


def send_verification_expiry_email(batch_verifications):
    """
    Spins a task to send verification expiry email to the learners in the batch using edx_ace
    If the email is successfully sent change the expiry_email_date to reflect when the
    email was sent
    """
    site = Site.objects.get(name=settings.SITE_NAME)
    message_context = get_base_template_context(site)
    message_context.update({
        'platform_name': settings.PLATFORM_NAME,
        'lms_verification_link': '{}{}'.format(settings.LMS_ROOT_URL, reverse("verify_student_reverify")),
        'help_center_link': settings.ID_VERIFICATION_SUPPORT_LINK
    })

    expiry_email = VerificationExpiry(context=message_context)

    for verification in batch_verifications:
        user = User.objects.get(id=verification.user_id)
        with emulate_http_request(site=site, user=user):
            msg = expiry_email.personalize(
                recipient=Recipient(user.username, user.email),
                language=get_user_preference(user, LANGUAGE_KEY),
                user_context={
                    'full_name': user.profile.name,
                }
            )
            ace.send(msg)
            verification_qs = SoftwareSecurePhotoVerification.objects.filter(pk=verification.pk)
            verification_qs.update(expiry_email_date=datetime.now(UTC))
