from argparse import ArgumentParser
from datetime import datetime, timedelta
from json import dump, load
from os import path as osPath, system
from time import sleep
from traceback import format_exc


def log(msg: str, logToFile: bool = True, end: str = None):
    date = f'[{datetime.now().strftime("%Y-%b-%d %H:%M:%S")}]'
    if logToFile:
        workFolder = osPath.split(__file__)[0].replace("\\", "/")

        if not osPath.exists(f"{workFolder}/BorgBackup.log"):
            with open(f"{workFolder}/BorgBackup.log", "w", encoding="UTF-8") as fileW:
                fileW.write("")

        with open(f"{workFolder}/BorgBackup.log", "a", encoding="UTF-8") as fileA:
            fileA.write(f'[{datetime.now().strftime("%Y-%b-%d %H:%M:%S")}] {msg.replace(chr(10), chr(10) + date + chr(32))}\n')

    print(f"{date} {msg.replace(chr(10), chr(10) + date + chr(32))}", end=end)


class timout:
    def __init__(self, bakWeeksOfMonth: list, bakDays: list, bakTime: str):
        self.bakWeeksOfMonth = bakWeeksOfMonth
        self.bakDays = bakDays
        self.bakTime = bakTime

    def getNextBackupDate(self):
        if not ":" in self.bakTime:
            raise Exception("Unable to set the correct time!")

        date = datetime.now().replace(microsecond=0)

        if len(self.bakTime.split(":")) > 2:
            isSecondCorrect = False
            for i in range(0, 61):
                if date.second == int(self.bakTime.split(":")[2]):
                    isSecondCorrect = True
                    break

                date += timedelta(seconds=1)
        else:
            date = date.replace(second=0, microsecond=0)
            isSecondCorrect = True
        if not isSecondCorrect:
            raise Exception("Unable to set the correct second!")

        isMinuteCorrect = False
        for i in range(0, 61):
            if date.minute == int(self.bakTime.split(":")[1]):
                isMinuteCorrect = True
                break

            date += timedelta(minutes=1)
        if not isMinuteCorrect:
            raise Exception("Unable to set the correct minute!")

        isHourCorrect = False
        for i in range(0, 25):
            if date.hour == int(self.bakTime.split(":")[0]):
                isHourCorrect = True
                break

            date += timedelta(hours=1)
        if not isHourCorrect:
            raise Exception("Unable to set the correct hour!")

        isWeekCorrect = False
        for i in range(0, 32):
            if int(((date.day - 1) / 7) + 1) in self.bakWeeksOfMonth:
                isWeekCorrect = True
                break

            date += timedelta(days=1)
        if not isWeekCorrect:
            raise Exception("Unable to set the correct week!")

        isWeekdayCorrect = False
        for i in range(0, 8):
            for day in self.bakDays:
                if date.weekday() == {"monday": 0, "tuesday": 1, "wednesday": 2, "thursday": 3, "friday": 4, "saturday": 5, "sunday": 6}[day.lower()]:
                    isWeekdayCorrect = True

            if isWeekdayCorrect:
                break

            date += timedelta(days=1)
        if not isWeekdayCorrect:
            raise Exception("Unable to set the correct weekday!")

        return date

    def sleep(self, dryRun: bool = False):
        nextBackupDate = self.getNextBackupDate()
        timeDiff = int(nextBackupDate.timestamp() - datetime.now().replace(microsecond=0).timestamp())

        log(f'Next backup scheduled for: {nextBackupDate.strftime("%Y-%b-%d %H:%M:%S")}')
        if dryRun:
            log("\n", logToFile=False)
            return None

        while timeDiff > 0:
            dateDiff = datetime.fromtimestamp(timeDiff) - timedelta(hours=1)
            log(f'Time left: {dateDiff.hour}:{dateDiff.strftime("%M:%S")} ({dateDiff.day - 1} Days)', logToFile=False, end="\r")

            timeDiff -= 1
            sleep(1)

            if timeDiff % 60 == 0:
                timeDiff = int(nextBackupDate.timestamp() - datetime.now().replace(microsecond=0).timestamp())

        log("\n", logToFile=False)


class borgBackup:
    def __init__(self, args):
        self.pathToRepos = args.repopath[0]
        self.pathToBorg = args.b
        self.compType = args.c
        self.shutdownWhenDone = not args.a
        self.dryRun = args.t

        defaultTemplate = {"RepoName": {"psw": "RepoPassword", "sources": ("/full/path/to/source/folder1",), "excludes": ("/full/*/to/exclude/folder1", None)}}
        workFolder = osPath.split(__file__)[0].replace("\\", "/")

        if not osPath.exists(f"{workFolder}/BorgBackup.json"):
            with open(f"{workFolder}/BorgBackup.json", "w", encoding="UTF-8") as fileW:
                dump(defaultTemplate, fileW, indent=2)

        with open(f"{workFolder}/BorgBackup.json", "r", encoding="UTF-8") as fileR:
            self.repos = load(fileR)

        if self.repos == defaultTemplate:
            log(f"Please configure the file: {workFolder}/BorgBackup.json")
            exit()

    def runBackUp(self, repo: str):
        if not repo in self.repos:
            raise Exception(f"Failed execution of: {repo}")

        args = ["export", f'BORG_PASSPHRASE="{self.repos[repo]["psw"]}"', "&&", self.pathToBorg, "create", "--list", "-v", "-p", "-C", self.compType, f'{self.pathToRepos}/{repo}::{datetime.now().strftime("%Y-%b-%d")}']

        if self.dryRun:
            args.append("--dry-run")

        if not self.repos[repo]["sources"] is None and type(self.repos[repo]["sources"]) is str:
            args += [f'"{self.repos[repo]["sources"]}"']
        elif not self.repos[repo]["sources"] is None and type(self.repos[repo]["sources"]) is tuple:
            for source in self.repos[repo]["sources"]:
                args += [f'"{source}"']

        if not self.repos[repo]["excludes"] is None and type(self.repos[repo]["excludes"]) is str:
            args += ["-e", f'"{self.repos[repo]["excludes"]}"']
        elif not self.repos[repo]["excludes"] is None and type(self.repos[repo]["excludes"]) is tuple:
            for excl in self.repos[repo]["excludes"]:
                args += ["-e", f'"{excl}"']

        argsWrapped = ("echo", f'\'{" ".join((*args, ))}\'', "|", "sh")
        out = system(" ".join(argsWrapped))
        if out != 0:
            raise Exception(f'Failed execution of: {" ".join(argsWrapped)}')
        log(f'Executed: {" ".join(argsWrapped)}')

    def main(self):
        timeout = timout(bakWeeksOfMonth=[1, 2, 3, 4, 5], bakDays=["monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday"], bakTime="03:00:00")

        while True:
            timeout.sleep(dryRun=self.dryRun)

            for repo in self.repos:
                try:
                    self.runBackUp(f"{repo}")
                except Exception:
                    log(f"Failed running backup for: {repo}\nError -> {format_exc()}")

            sleep(90)

            if self.shutdownWhenDone:
                if self.dryRun:
                    log("Execute: sudo shutdown now", logToFile=False)
                    continue
                system("sudo shutdown now")


if __name__ == "__main__":
    parser = ArgumentParser(description="BorgBackUp sceduler and setup.")
    parser.add_argument("repopath", default=["/disk1"], metavar="RepoPath", nargs=1, help="Specify where the repos are stored.")
    parser.add_argument("-b", "-borgpath", default="/bin/borg", help="Specify where borg is located.")
    parser.add_argument("-a", "-awake", action="store_true", help="Prohibit shutdown of the system after the next backup has completed.")
    parser.add_argument("-t", "-test", action="store_true", help="Test this script, no changes will be made.")
    parser.add_argument(
        "-c", "-compresion", default="zstd,22", choices=("none", "lz4", *(f"zstd,{i}" for i in range(1, 23)), *(f"zlib,{i}" for i in range(0, 10)), *(f"lzma,{i}" for i in range(0, 10))), help="The compression algorithm that needs to be used."
    )
    args = parser.parse_args()

    borgBackup(args).main()
