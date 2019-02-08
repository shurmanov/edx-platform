"""
Tests for django admin command `send_verification_expiry_email` in the verify_student module
"""

from datetime import datetime, timedelta

import boto
from django.conf import settings
from django.core import mail
from django.core.management import call_command
from django.test import TestCase
from django.contrib.sites.models import Site
from mock import patch
from pytz import UTC
from student.tests.factories import UserFactory
from testfixtures import LogCapture

from common.test.utils import MockS3Mixin
from lms.djangoapps.verify_student.models import SoftwareSecurePhotoVerification
from lms.djangoapps.verify_student.tests.test_models import (
    FAKE_SETTINGS,
    mock_software_secure_post
)

LOGGER_NAME = 'lms.djangoapps.verify_student.management.commands.send_verification_expiry_email'


@patch.dict(settings.VERIFY_STUDENT, FAKE_SETTINGS)
@patch('lms.djangoapps.verify_student.models.requests.post', new=mock_software_secure_post)
class TestSendVerificationExpiryEmail(MockS3Mixin, TestCase):
    """ Tests for django admin command `send_verification_expiry_email` in the verify_student module """

    def setUp(self):
        """ Initial set up for tests """
        super(TestSendVerificationExpiryEmail, self).setUp()
        connection = boto.connect_s3()
        connection.create_bucket(FAKE_SETTINGS['SOFTWARE_SECURE']['S3_BUCKET'])
        Site.objects.create(domain='edx.org', name='edx.org')

    def create_and_submit(self, user):
        """ Helper method that lets us create new SoftwareSecurePhotoVerifications """
        attempt = SoftwareSecurePhotoVerification(user=user)
        attempt.upload_face_image("Fake Data")
        attempt.upload_photo_id_image("More Fake Data")
        attempt.mark_ready()
        attempt.submit()
        return attempt

    def test_expiry_date_range(self):
        """
        Test that the verifications are filtered on the given range. Email is not sent for any verification with
        expiry date out of range
        """
        user = UserFactory.create()
        verification_in_range = self.create_and_submit(user)
        verification_in_range.status = 'approved'
        verification_in_range.expiry_date = datetime.now(UTC) - timedelta(days=1)
        verification_in_range.save()

        user = UserFactory.create()
        verification = self.create_and_submit(user)
        verification.status = 'approved'
        verification.expiry_date = datetime.now(UTC) - timedelta(days=5)
        verification.save()

        call_command('send_verification_expiry_email', '--days-range=2')

        # Check that only one email is sent
        self.assertEqual(len(mail.outbox), 1)

        # Verify that the email is not sent to the out of range verification
        expiry_email_date = SoftwareSecurePhotoVerification.objects.get(pk=verification.pk).expiry_email_date
        self.assertIsNone(expiry_email_date)

    def test_expiry_date_null(self):
        """
        Test that the SoftwareSecurePhotoVerification object is not filtered if it's expiry_date is set
        to NULL
        """
        # for outdated verification the expiry_date is set NULL verify_student/views.py:1164
        user = UserFactory.create()
        outdated_verification = self.create_and_submit(user)
        outdated_verification.status = 'approved'
        outdated_verification.save()

        call_command('send_verification_expiry_email')

        # Check that the expiry_email_date is not set for the outdated expired verification
        expiry_email_date = SoftwareSecurePhotoVerification.objects.get(pk=outdated_verification.pk).expiry_email_date
        self.assertIsNone(expiry_email_date)

    def test_send_verification_expiry_email(self):
        """
        Test that checks for valid criteria the email is sent and expiry_email_date is set
        """
        user = UserFactory.create()
        verification = self.create_and_submit(user)
        verification.status = 'approved'
        verification.expiry_date = datetime.now(UTC) - timedelta(days=1)
        verification.save()

        call_command('send_verification_expiry_email')

        expected_date = datetime.now(UTC)
        attempt = SoftwareSecurePhotoVerification.objects.get(user_id=verification.user_id)
        self.assertEquals(attempt.expiry_email_date.date(), expected_date.date())
        self.assertEqual(len(mail.outbox), 1)

    def test_no_verification_found(self):
        """
        Test that if no approved and expired verifications are found the management command terminates gracefully
        """

        start_date = datetime.now(UTC) - timedelta(days=365)  # using default days
        with LogCapture(LOGGER_NAME) as logger:
            call_command('send_verification_expiry_email')
            logger.check(
                (LOGGER_NAME,
                 'INFO', "No approved expired entries found in SoftwareSecurePhotoVerification for the "
                         "date range {} - {}".format(start_date.date(), datetime.now(UTC).date()))
            )

    def test_dry_run_flag(self):
        """
        Test that the dry run flags sends no email and only logs the ranges of user ids for the batches
        """
        user = UserFactory.create()
        verification = self.create_and_submit(user)
        verification.status = 'approved'
        verification.expiry_date = datetime.now(UTC) - timedelta(days=1)
        verification.save()

        start_date = datetime.now(UTC) - timedelta(days=365)  # using default days

        with LogCapture(LOGGER_NAME) as logger:
            call_command('send_verification_expiry_email', '--dry-run')
            logger.check(
                (LOGGER_NAME,
                 'INFO',
                 "For the date range {} - {}, total Software Secure Photo verification filtered are {}"
                 .format(start_date.date(), datetime.now(UTC).date(), 1)
                 ),
                (LOGGER_NAME,
                 'INFO',
                 "This was a dry run, no email was sent. For the actual run email would have been "
                 "sent for expired verification within the range: user id {} - user id {}".format(
                     user.id, user.id)
                 ))
        self.assertEqual(len(mail.outbox), 0)
