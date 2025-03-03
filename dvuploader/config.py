import os


def config(
    max_retries: int = 15,
    max_retry_time: int = 240,
    min_retry_time: int = 1,
    retry_multiplier: float = 0.1,
    max_package_size: int = 2 * 1024**3,
):
    """This function sets the environment variables for the dvuploader package.

    Use this function to set the environment variables for the dvuploader package,
    which controls the behavior of the package. This is particularly useful when
    you want to be more loose on the handling of the retry logic and upload size.

    Retry logic:
        Native uploads in particular may be subject to intermediate locks
        on the Dataverse side, which may cause the upload to fail. We provide
        and exponential backoff mechanism to deal with this.

        The exponential backoff is controlled by the following environment variables:
            - DVUPLOADER_MAX_RETRIES: The maximum number of retries.
            - DVUPLOADER_MAX_RETRY_TIME: The maximum retry time.
            - DVUPLOADER_MIN_RETRY_TIME: The minimum retry time.
            - DVUPLOADER_RETRY_MULTIPLIER: The retry multiplier.

        The recursive formula for the wait time is:
            wait_time = min_retry_time * retry_multiplier^n
            where n is the number of retries.

        The wait time will not exceed max_retry_time.

    Upload size:
        The maximum upload size is controlled by the following environment variable:
            - DVUPLOADER_MAX_PKG_SIZE: The maximum package size.

        The default maximum package size is 2GB, but this can be changed by
        setting the DVUPLOADER_MAX_PKG_SIZE environment variable.

        We recommend not to exceed 2GB, as this is the maximum size supported
        by Dataverse and beyond that the risk of failure increases.

    Args:
        max_retries (int): The maximum number of retries.
        max_retry_time (int): The maximum retry time.
        min_retry_time (int): The minimum retry time.
        retry_multiplier (float): The retry multiplier.
        max_package_size (int): The maximum package size.
    """

    os.environ["DVUPLOADER_MAX_RETRIES"] = str(max_retries)
    os.environ["DVUPLOADER_MAX_RETRY_TIME"] = str(max_retry_time)
    os.environ["DVUPLOADER_MIN_RETRY_TIME"] = str(min_retry_time)
    os.environ["DVUPLOADER_RETRY_MULTIPLIER"] = str(retry_multiplier)
    os.environ["DVUPLOADER_MAX_PKG_SIZE"] = str(max_package_size)
