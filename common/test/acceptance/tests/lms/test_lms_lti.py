# -*- coding: utf-8 -*-
"""
Bok choy acceptance tests for LTI xblock
See also old lettuce tests in lms/djangoapps/courseware/features/lti.feature
"""
from ...fixtures.course import CourseFixture, XBlockFixtureDesc
from ...pages.lms.courseware import CoursewarePage
from ...pages.studio.overview import CourseOutlinePage as StudioCourseOutlinePage
from ..helpers import UniqueCourseTest, auto_auth
import os


class TestLTIConusmer(UniqueCourseTest):
    """
    Base class for tests of LTI xblock in the LMS.
    """

    USERNAME = "STUDENT_TESTER"
    EMAIL = "student101@example.com"
    host = os.environ.get('BOK_CHOY_HOSTNAME', '127.0.0.1')

    def setUp(self):
        super(TestLTIConusmer, self).setUp()
        self.courseware_page = CoursewarePage(self.browser, self.course_id)
        self.studio_course_outline = StudioCourseOutlinePage(
            self.browser,
            self.course_info['org'],
            self.course_info['number'],
            self.course_info['run']
        )
        # Install a course

        self.course_fix = CourseFixture(
            self.course_info['org'], self.course_info['number'],
            self.course_info['run'], self.course_info['display_name']
        )
        # self.course_fix.visit()

    def test_lti_is_not_rendered(self):
        """
        Scenario: LTI component in LMS with no launch_url is not rendered
        Given the course has correct LTI credentials with registered Instructor
            the course has an LTI component with no_launch_url fields:
            Then I view the LTI and error is shown
        """
        metadata_advance_settings = "correct_lti_id:test_client_key:test_client_secret"
        metadata_lti_xblock = {
            'lti_id': '',
            'launch_url': '',
        }
        self.set_advance_settings(metadata_advance_settings)
        self.create_lti_xblock(metadata_lti_xblock)
        auto_auth(self.browser, self.USERNAME, self.EMAIL, False, self.course_id)
        self.courseware_page.visit()
        self.assertTrue(self.courseware_page.is_error_message_present())
        self.assertFalse(self.courseware_page.is_iframe_present())
        self.assertFalse(self.courseware_page.is_launch_url_present())

    def test_incorrect_lti_id_is_rendered(self):
        """
        Scenario: LTI component in LMS with incorrect lti_id is rendered incorrectly
        Given the course has correct LTI credentials with registered Instructor
            the course has an LTI component with incorrect_lti_id fields:
            Then I view the LTI but incorrect_signature warning is rendered
        """
        metadata_advance_settings = "test_lti_id:test_client_key:test_client_secret"
        metadata_lti_xblock = {
            'lti_id': 'incorrect_lti_id',
            'launch_url': 'http://{}:{}/{}'.format(self.host, '8765', 'correct_lti_endpoint')
        }
        self.set_advance_settings(metadata_advance_settings)
        self.create_lti_xblock(metadata_lti_xblock)
        auto_auth(self.browser, self.USERNAME, self.EMAIL, False, self.course_id)
        self.courseware_page.visit()
        self.assertTrue(self.courseware_page.is_iframe_present())
        self.assertFalse(self.courseware_page.is_launch_url_present())
        self.assertFalse(self.courseware_page.is_error_message_present())
        self.assertEqual("Wrong LTI signature", self.courseware_page.lti_content)

    def test_incorrect_lti_credentials_is_rendered(self):
        """
        Scenario: LTI component in LMS with icorrect LTI credentials is rendered incorrectly
        Given the course has incorrect LTI credentials with registered Instructor
            the course has an LTI component with correct fields:
            I view the LTI but incorrect_signature warning is rendered
        """
        metadata_advance_settings = "test_lti_id:test_client_key:incorrect_lti_secret_key"
        metadata_lti_xblock = {
            'lti_id': 'correct_lti_id',
            'launch_url': 'http://{}:{}/{}'.format(self.host, '8765', 'correct_lti_endpoint')
        }
        self.set_advance_settings(metadata_advance_settings)
        self.create_lti_xblock(metadata_lti_xblock)
        auto_auth(self.browser, self.USERNAME, self.EMAIL, False, self.course_id)
        self.courseware_page.visit()
        self.assertTrue(self.courseware_page.is_iframe_present())
        self.assertFalse(self.courseware_page.is_launch_url_present())
        self.assertFalse(self.courseware_page.is_error_message_present())
        self.assertEqual("Wrong LTI signature", self.courseware_page.lti_content)

    def test_lti_is_rendered_iframe(self):
        """
        Scenario: LTI component in LMS is correctly rendered in iframe
        Given the course has correct LTI credentials with registered Instructor
            the course has an LTI component with correct fields:
            I view the LTI and it is rendered in iframe
        """
        metadata_advance_settings = "correct_lti_id:test_client_key:test_client_secret"
        metadata_lti_xblock = {
            'lti_id': 'correct_lti_id',
            'launch_url': 'http://{}:{}/{}'.format(self.host, '8765', 'correct_lti_endpoint')
        }
        self.set_advance_settings(metadata_advance_settings)
        self.create_lti_xblock(metadata_lti_xblock)
        auto_auth(self.browser, self.USERNAME, self.EMAIL, True, self.course_id)
        self.courseware_page.visit()
        self.assertTrue(self.courseware_page.is_iframe_present())
        self.assertFalse(self.courseware_page.is_launch_url_present())
        self.assertFalse(self.courseware_page.is_error_message_present())
        self.assertEqual("This is LTI tool. Success.", self.courseware_page.lti_content)

    def test_lti_grading_for_staff(self):
        """
        Scenario: LTI component in LMS is correctly rendered in iframe
        Given the course has correct LTI credentials with registered Instructor
            the course has an LTI component with correct fields:
            I view the LTI and it is rendered in iframe
        """
        metadata_advance_settings = "correct_lti_id:test_client_key:test_client_secret"
        metadata_lti_xblock = {
            'lti_id': 'correct_lti_id',
            'launch_url': 'http://{}:{}/{}'.format(self.host, '8765', 'correct_lti_endpoint')
        }
        self.set_advance_settings(metadata_advance_settings)
        self.create_lti_xblock(metadata_lti_xblock)
        auto_auth(self.browser, self.USERNAME, self.EMAIL, True, self.course_id)
        self.courseware_page.visit()
        self.courseware_page.submit_lti_answer("#submit-button")
        self.assertIn("LTI consumer (edX) responded with XML content", self.courseware_page.lti_content)

    def set_advance_settings(self, metadata):

        # Set word cloud value against advanced modules in advanced settings
        self.course_fix.add_advanced_settings({
            "advanced_modules": {"value": ["lti_consumer"]},
            'lti_passports': {"value": [metadata]}
        })

    def create_lti_xblock(self, metadata_lti_xblock):
        self.course_fix.add_children(
            XBlockFixtureDesc('chapter', 'Test Section 1').add_children(
                XBlockFixtureDesc('sequential', 'Test Subsection 1').add_children(
                    XBlockFixtureDesc('vertical', 'Test Unit').add_children(
                        XBlockFixtureDesc(
                            'lti_consumer', 'advanced LTI Consumer',
                            metadata=metadata_lti_xblock
                        )
                    )
                )
            )
        ).install()
