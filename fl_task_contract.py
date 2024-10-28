import asyncio
import json
from typing import List

from tenacity import retry, stop_after_attempt, wait_fixed
from eth_account.signers.local import LocalAccount
from pydantic import BaseModel
from web3 import AsyncHTTPProvider, AsyncWeb3, HTTPProvider, Web3
from web3.middleware import SignAndSendRawMiddlewareBuilder
from web3.types import TxParams, TxReceipt, EventData
from web3.logs import DISCARD


class Task(BaseModel):
    task_id: int
    creator: str
    max_worker_count: int
    max_round: int
    curr_round: int
    workers: List[str]
    data_uploaded: List[bool]
    data_aggregated: bool


def get_abi():
    with open("abi/FLTask.json", mode="r") as f:
        content = json.load(f)
    return content["abi"], content["bytecode"]


_abi, _bytecode = get_abi()

_default_tx_option: TxParams = {
    "gasPrice": Web3.to_wei(10, "wei"),
    "gas": 5000000,
}


def deploy_fl_task(url: str, privkey: str) -> str:
    w3 = Web3(HTTPProvider(url))
    if privkey.startswith("0x"):
        privkey = privkey[2:]
    pk_bytes = bytes.fromhex(privkey)
    acct: LocalAccount = w3.eth.account.from_key(pk_bytes)
    w3.middleware_onion.inject(SignAndSendRawMiddlewareBuilder.build(acct), layer=0)  # type: ignore
    contract = w3.eth.contract(abi=_abi, bytecode=_bytecode)

    nonce = w3.eth.get_transaction_count(
        account=acct.address, block_identifier="pending"
    )
    opt: TxParams = {}
    opt.update(**_default_tx_option)
    opt["nonce"] = nonce
    opt["from"] = acct.address
    tx_hash = contract.constructor().transact(opt)
    receipt = w3.eth.wait_for_transaction_receipt(tx_hash)
    address = receipt["contractAddress"]
    assert address is not None
    return address


class FLTask(object):
    def __init__(self, url: str, privkey: str, contract_address: str):
        self.w3 = Web3(HTTPProvider(url))
        if privkey.startswith("0x"):
            privkey = privkey[2:]
        pk_bytes = bytes.fromhex(privkey)
        self.acct: LocalAccount = self.w3.eth.account.from_key(pk_bytes)
        self.w3.middleware_onion.inject(SignAndSendRawMiddlewareBuilder.build(self.acct), layer=0)  # type: ignore
        self.contract = self.w3.eth.contract(
            abi=_abi, address=Web3.to_checksum_address(contract_address)
        )

    @retry(stop=stop_after_attempt(3), wait=wait_fixed(3))
    def create_task(self, worker_count: int, max_round: int):
        nonce = self.w3.eth.get_transaction_count(
            account=self.acct.address, block_identifier="pending"
        )
        opt: TxParams = {}
        opt.update(**_default_tx_option)
        opt["nonce"] = nonce
        opt["from"] = self.acct.address
        tx_hash = self.contract.functions.createTask(
            workerCount=worker_count, maxRound=max_round
        ).transact(opt)
        print(f"create task tx hash: {tx_hash.hex()}")
        receipt = self.w3.eth.wait_for_transaction_receipt(tx_hash)
        return receipt

    @retry(stop=stop_after_attempt(3), wait=wait_fixed(3))
    def join_task(self, task_id: int):
        nonce = self.w3.eth.get_transaction_count(
            account=self.acct.address, block_identifier="pending"
        )
        opt: TxParams = {}
        opt.update(**_default_tx_option)
        opt["nonce"] = nonce
        opt["from"] = self.acct.address
        tx_hash = self.contract.functions.joinTask(taskID=task_id).transact(opt)
        print(f"join task tx hash: {tx_hash.hex()}")
        receipt = self.w3.eth.wait_for_transaction_receipt(tx_hash)
        return receipt

    @retry(stop=stop_after_attempt(3), wait=wait_fixed(3))
    def start_task(self, task_id: int):
        nonce = self.w3.eth.get_transaction_count(
            account=self.acct.address, block_identifier="pending"
        )
        opt: TxParams = {}
        opt.update(**_default_tx_option)
        opt["nonce"] = nonce
        opt["from"] = self.acct.address
        tx_hash = self.contract.functions.startTask(taskID=task_id).transact(opt)
        print(f"start task tx hash: {tx_hash.hex()}")
        receipt = self.w3.eth.wait_for_transaction_receipt(tx_hash)
        return receipt

    @retry(stop=stop_after_attempt(3), wait=wait_fixed(3))
    def start_round(self, task_id: int, round: int):
        nonce = self.w3.eth.get_transaction_count(
            account=self.acct.address, block_identifier="pending"
        )
        opt: TxParams = {}
        opt.update(**_default_tx_option)
        opt["nonce"] = nonce
        opt["from"] = self.acct.address
        tx_hash = self.contract.functions.startRound(
            taskID=task_id, round=round
        ).transact(opt)
        print(f"start round tx hash: {tx_hash.hex()}")
        receipt = self.w3.eth.wait_for_transaction_receipt(tx_hash)
        return receipt

    @retry(stop=stop_after_attempt(3), wait=wait_fixed(3))
    def upload_data(self, task_id: int, round: int, worker_id: int):
        nonce = self.w3.eth.get_transaction_count(
            account=self.acct.address, block_identifier="pending"
        )
        opt: TxParams = {}
        opt.update(**_default_tx_option)
        opt["nonce"] = nonce
        opt["from"] = self.acct.address
        tx_hash = self.contract.functions.uploadData(
            taskID=task_id, round=round, workerID=worker_id
        ).transact(opt)
        print(f"upload data tx hash: {tx_hash.hex()}")
        receipt = self.w3.eth.wait_for_transaction_receipt(tx_hash)
        return receipt

    @retry(stop=stop_after_attempt(3), wait=wait_fixed(3))
    def aggregate_data(self, task_id: int, round: int):
        nonce = self.w3.eth.get_transaction_count(
            account=self.acct.address, block_identifier="pending"
        )
        opt: TxParams = {}
        opt.update(**_default_tx_option)
        opt["nonce"] = nonce
        opt["from"] = self.acct.address
        tx_hash = self.contract.functions.aggregateData(
            taskID=task_id, round=round
        ).transact(opt)
        print(f"aggregate data tx hash: {tx_hash.hex()}")
        receipt = self.w3.eth.wait_for_transaction_receipt(tx_hash)
        return receipt

    @retry(stop=stop_after_attempt(3), wait=wait_fixed(3))
    def finish_task(self, task_id: int):
        nonce = self.w3.eth.get_transaction_count(
            account=self.acct.address, block_identifier="pending"
        )
        opt: TxParams = {}
        opt.update(**_default_tx_option)
        opt["nonce"] = nonce
        opt["from"] = self.acct.address
        tx_hash = self.contract.functions.finishTask(taskID=task_id).transact(opt)
        print(f"finish task tx hash: {tx_hash.hex()}")
        receipt = self.w3.eth.wait_for_transaction_receipt(tx_hash)
        return receipt

    @retry(stop=stop_after_attempt(3), wait=wait_fixed(3))
    def get_task(self, task_id: int):
        res = self.contract.functions.getTask(taskID=task_id).call(
            {"from": self.acct.address}
        )
        return Task(
            task_id=res[0],
            creator=res[1],
            max_worker_count=res[2],
            max_round=res[3],
            curr_round=res[4],
            workers=res[5],
            data_uploaded=res[6],
            data_aggregated=res[7],
        )

    def process_event(self, event_name: str, receipt: TxReceipt) -> List[EventData]:
        events = list(
            self.contract.events[event_name]().process_receipt(
                receipt, errors=DISCARD
            )
        )
        return events


class AsyncFLTask(object):
    def __init__(self, url: str, privkey: str, contract_address: str):
        self.w3 = AsyncWeb3(AsyncHTTPProvider(url))
        if privkey.startswith("0x"):
            privkey = privkey[2:]
        pk_bytes = bytes.fromhex(privkey)
        self.acct: LocalAccount = self.w3.eth.account.from_key(pk_bytes)
        self.w3.middleware_onion.inject(SignAndSendRawMiddlewareBuilder.build(self.acct), layer=0)  # type: ignore
        self.contract = self.w3.eth.contract(
            abi=_abi, address=Web3.to_checksum_address(contract_address)
        )
        self._lock = asyncio.Lock()

    @retry(stop=stop_after_attempt(3), wait=wait_fixed(3))
    async def create_task(self, worker_count: int, max_round: int):
        async with self._lock:
            nonce = await self.w3.eth.get_transaction_count(
                account=self.acct.address, block_identifier="pending"
            )
            opt: TxParams = {}
            opt.update(**_default_tx_option)
            opt["nonce"] = nonce
            opt["from"] = self.acct.address
            tx_hash = await self.contract.functions.createTask(
                workerCount=worker_count, maxRound=max_round
            ).transact(opt)
        receipt = await self.w3.eth.wait_for_transaction_receipt(tx_hash)
        return receipt

    @retry(stop=stop_after_attempt(3), wait=wait_fixed(3))
    async def join_task(self, task_id: int):
        async with self._lock:
            nonce = await self.w3.eth.get_transaction_count(
                account=self.acct.address, block_identifier="pending"
            )
            opt: TxParams = {}
            opt.update(**_default_tx_option)
            opt["nonce"] = nonce
            opt["from"] = self.acct.address
            tx_hash = await self.contract.functions.joinTask(taskID=task_id).transact(opt)
        receipt = await self.w3.eth.wait_for_transaction_receipt(tx_hash)
        return receipt

    @retry(stop=stop_after_attempt(3), wait=wait_fixed(3))
    async def start_task(self, task_id: int):
        async with self._lock:
            nonce = await self.w3.eth.get_transaction_count(
                account=self.acct.address, block_identifier="pending"
            )
            opt: TxParams = {}
            opt.update(**_default_tx_option)
            opt["nonce"] = nonce
            opt["from"] = self.acct.address
            tx_hash = await self.contract.functions.startTask(taskID=task_id).transact(opt)
        receipt = await self.w3.eth.wait_for_transaction_receipt(tx_hash)
        return receipt

    @retry(stop=stop_after_attempt(3), wait=wait_fixed(3))
    async def start_round(self, task_id: int, round: int):
        async with self._lock:
            nonce = await self.w3.eth.get_transaction_count(
                account=self.acct.address, block_identifier="pending"
            )
            opt: TxParams = {}
            opt.update(**_default_tx_option)
            opt["nonce"] = nonce
            opt["from"] = self.acct.address
            tx_hash = await self.contract.functions.startRound(
                taskID=task_id, round=round
            ).transact(opt)
        receipt = await self.w3.eth.wait_for_transaction_receipt(tx_hash)
        return receipt

    @retry(stop=stop_after_attempt(3), wait=wait_fixed(3))
    async def upload_data(self, task_id: int, round: int, worker_id: int):
        async with self._lock:
            nonce = await self.w3.eth.get_transaction_count(
                account=self.acct.address, block_identifier="pending"
            )
            opt: TxParams = {}
            opt.update(**_default_tx_option)
            opt["nonce"] = nonce
            opt["from"] = self.acct.address
            tx_hash = await self.contract.functions.uploadData(
                taskID=task_id, round=round, workerID=worker_id
            ).transact(opt)
        receipt = await self.w3.eth.wait_for_transaction_receipt(tx_hash)
        return receipt

    @retry(stop=stop_after_attempt(3), wait=wait_fixed(3))
    async def aggregate_data(self, task_id: int, round: int):
        async with self._lock:
            nonce = await self.w3.eth.get_transaction_count(
                account=self.acct.address, block_identifier="pending"
            )
            opt: TxParams = {}
            opt.update(**_default_tx_option)
            opt["nonce"] = nonce
            opt["from"] = self.acct.address
            tx_hash = await self.contract.functions.aggregateData(
                taskID=task_id, round=round
            ).transact(opt)
        receipt = await self.w3.eth.wait_for_transaction_receipt(tx_hash)
        return receipt

    @retry(stop=stop_after_attempt(3), wait=wait_fixed(3))
    async def finish_task(self, task_id: int):
        async with self._lock:
            nonce = await self.w3.eth.get_transaction_count(
                account=self.acct.address, block_identifier="pending"
            )
            opt: TxParams = {}
            opt.update(**_default_tx_option)
            opt["nonce"] = nonce
            opt["from"] = self.acct.address
            tx_hash = await self.contract.functions.finishTask(taskID=task_id).transact(opt)
        receipt = await self.w3.eth.wait_for_transaction_receipt(tx_hash)
        return receipt
    
    @retry(stop=stop_after_attempt(3), wait=wait_fixed(3))
    async def get_task(self, task_id: int):
        res = await self.contract.functions.getTask(taskID=task_id).call(
            {"from": self.acct.address}
        )
        return Task(
            task_id=res[0],
            creator=res[1],
            max_worker_count=res[2],
            max_round=res[3],
            curr_round=res[4],
            workers=res[5],
            data_uploaded=res[6],
            data_aggregated=res[7],
        )

    def process_event(self, event_name: str, receipt: TxReceipt) -> List[EventData]:
        events = list(
            self.contract.events[event_name]().process_receipt(
                receipt, errors=DISCARD
            )
        )
        return events


if __name__ == "__main__":
    contract_address = deploy_fl_task(
        url="https://crynux-testnet.public.blastapi.io",
        privkey="0x294433a113bbec5fb430219cb8214c7fc8638f01a12cceb6eacad7ce22abfe6d",
    )
    print(contract_address)
