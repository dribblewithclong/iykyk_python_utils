import re
import sys
import warnings
import asyncio
import aiohttp
from asynciolimiter import Limiter

sys.path.append(
    re.search(
        f'.*{re.escape("iykyk_python_utils")}',
        __file__,
    ).group()
)

warnings.filterwarnings('ignore')

from logging_iyk.logger import Logger       # noqa: E402


class AsyncAPIRequest:
    def __init__(
        self,
        limit_rate: float = 64/1,
        succeed_codes: list = [200],
    ):
        self.logging = Logger()

        self.limiter = Limiter(limit_rate)

        self.succeed_codes = succeed_codes

        self.req_made = 0
        self.resp_received = 0

        self.error_request_params = list()

    async def fetch(
        self,
        session: aiohttp.ClientSession,
        request_params: dict,
        method: str = 'get',
        return_url: bool = False,
        return_headers: bool = False,
        return_payload: bool = False,
        resp_as_text: bool = False,
    ) -> dict:
        request = {
            'get': session.get,
            'post': session.post,
        }

        await self.limiter.wait()

        if (
            self.req_made % 32 == 0
        ) & (
            self.req_made > 0
        ):
            self.logging.info(
                f'requests made: {self.req_made}'
            )
        if (
            self.resp_received % 32 == 0
        ) & (
            self.resp_received > 0
        ):
            self.logging.info(
                f'response received: {self.resp_received}'
            )

        self.req_made += 1
        resp = await request[method](
            url=request_params.get('url'),
            headers=request_params.get('headers'),
            json=request_params.get('payload'),
        )
        status_code = resp.status
        self.resp_received += 1

        if status_code in self.succeed_codes:
            if resp_as_text:
                resp_json = {
                    'data': await resp.content.read(),
                }
            else:
                resp_json = await resp.json()
        else:
            resp_json = {
                'error_code': status_code,
                'error_resp': await resp.content.read(),
            }
            self.error_request_params.append(
                {
                    'code': status_code,
                    'url': request_params.get('url'),
                    'headers': request_params.get('headers'),
                    'payload': request_params.get('payload'),
                }
            )

        resp_json['request_params'] = {}
        if return_url:
            resp_json['request_params']['url'] \
                = request_params.get('url')
        if return_headers:
            resp_json['request_params']['headers'] \
                = request_params.get('headers')
        if return_payload:
            resp_json['request_params']['payload'] \
                = request_params.get('payload')

        return resp_json

    async def fetchall(
        self,
        request_params_li: list,
        method: str = 'get',
        cookies: dict = None,
        return_url: bool = False,
        return_headers: bool = False,
        return_payload: bool = False,
        resp_as_text: bool = False,
    ) -> dict:
        async with aiohttp.ClientSession(
            cookies=cookies,
        ) as session:
            self.logging.info(
                f'total requests ahead: {len(request_params_li)}'
            )

            output_li = await asyncio.gather(
                *(
                    self.fetch(
                        session=session,
                        request_params=params,
                        method=method,
                        return_url=return_url,
                        return_headers=return_headers,
                        return_payload=return_payload,
                        resp_as_text=resp_as_text,
                    ) for params in request_params_li
                )
            )

            return {
                'succeed': output_li,
                'error_request_params': self.error_request_params,
            }

    def get_all_resp(
        self,
        request_params_li: list,
        method: str = 'get',
        cookies: dict = None,
        return_url: bool = False,
        return_headers: bool = False,
        return_payload: bool = False,
        resp_as_text: bool = False,
    ) -> dict:
        output_di = asyncio.run(
            self.fetchall(
                request_params_li,
                method,
                cookies,
                return_url,
                return_headers,
                return_payload,
                resp_as_text,
            )
        )

        self.req_made = 0
        self.resp_received = 0

        del self.error_request_params
        self.error_request_params = list()

        return output_di
