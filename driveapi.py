from __future__ import annotations
import os.path
from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from googleapiclient.http import MediaFileUpload
from googleapiclient.http import MediaIoBaseDownload
import io
import mimetypes
from typing import Union, List
from os.path import exists
import shutil
import time


CLIENT_SECRET = 'client_secret.json'
ACCESS_TOKEN = 'token.json'
SCOPES = ['https://www.googleapis.com/auth/drive']


def get_creds(token=ACCESS_TOKEN, client=CLIENT_SECRET):
    """Pega as credenciais via token.json se existir ou cria um por meio de oauth usando client_secret.json.

    :parameter token: Caminho para o arquivo token do tipo .json.
    :parameter client: Caminho para o arquivo client secret do tipo .json.

    :returns: Credenciais válidas.
    """
    creds = None

    if os.path.exists(token):
        try:
            creds = Credentials.from_authorized_user_file(token, SCOPES)
        except:
            os.remove(token)
            get_creds()

    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            try:
                creds.refresh(Request())
            except:
                os.remove(token) if exists(token) else ''
                return get_creds()

        else:
            flow = InstalledAppFlow.from_client_secrets_file(
                client, SCOPES)
            creds = flow.run_local_server(port=0)

        with open(token, 'w') as token:
            token.write(creds.to_json())
    return creds


def aut_drive():
    global creds
    serv = build('drive', 'v3', credentials=creds, cache_discovery=False)
    return serv


class File:
    def __init__(self, folder, file):
        self.service = service_d
        self.father = folder
        self.name = file[0]
        self.id = file[1]

    def __repr__(self):
        return self.id

    def __eq__(self, other):
        return self.id == other.id

    def download(self, path=os.getcwd()):
        if '.' in self.name:
            request = self.service.files().get_media(fileId=self.id)
            fh = io.BytesIO()
            downloader = MediaIoBaseDownload(fh, request)
            done = False
            while done is False:
                status, done = downloader.next_chunk()
        else:
            request = self.service.files().export_media(fileId=self.id,
                                                        mimeType='text/plain')
            fh = io.BytesIO()
            downloader = MediaIoBaseDownload(fh, request)
            done = False
            while done is False:
                status, done = downloader.next_chunk()
        name = self.name if '.' in self.name else self.name+'.txt'
        with open(fr"{path}\{name}", 'wb') as f:
            f.write(fh.getvalue())
        return fr"{path}\{name}"

    def read(self) -> str:
        if '.' in self.name:
            if self.name.split('.')[-1] not in ['csv', 'txt', 'json']:
                raise Exception('This file is not readable')
            request = self.service.files().get_media(fileId=self.id)
            fh = io.BytesIO()
            downloader = MediaIoBaseDownload(fh, request)
            done = False
            while done is False:
                status, done = downloader.next_chunk()
        else:
            request = self.service.files().export_media(fileId=self.id,
                                                        mimeType='text/plain')
            fh = io.BytesIO()
            downloader = MediaIoBaseDownload(fh, request)
            done = False
            while done is False:
                status, done = downloader.next_chunk()
        return fh.getvalue().decode('utf-8')


class Folder:
    def __init__(self, father, folder):
        self.father = father
        self.service = service_d
        self.name = folder[0]
        self.id = folder[1]
        self.__folders = []
        self.__files = []
        global root
        self.root = root

    def __repr__(self):
        return self.id

    def __check_file(self, name: str) -> Union[File, bool]:
        """Check whether a file is in the current folder.

        :parameter name: Name of the file.
        :return: False if the file is not in current folder and a File object if it is
        """

        return self.search(name, ftype='file')

    def __check_folder(self, name: str) -> Union[Folder, bool]:
        """Check whether a folder is in the current folder.

        :parameter name: Name of the folder.
        :return: False if the folder is not in the current folder and a Folder object if it is
        """

        return self.search(name, ftype='folder')

    def upload(self, path: str) -> File:
        """Upload a file to the current folder

        :parameter path: Path to a file that will be uploaded
        :return: File object that was uploaded
        """

        name = path if '/' not in path else path.split('/')[-1]
        name = name if '\\' not in name else name.split('\\')[-1]

        check = self.__check_file(name)
        if check:
            return check
        file_metadata = {
            'name': name,
            'parents': [self.id]
        }

        mimetype = mimetypes.guess_type(path, strict=True)

        media = MediaFileUpload(path,
                                mimetype=mimetype[0],
                                resumable=True)
        response = self.service.files().create(body=file_metadata,
                                               media_body=media,
                                               fields='name, id').execute()
        return File(self, tuple([response.get('name'), response.get('id')]))

    def upload_folder(self, path: str) -> Folder:
        """Upload a file to the current folder

        :parameter path: Path to a file that will be uploaded
        :return: File object that was uploaded
        """

        name = path if '/' not in path else path.split('/')[-1]
        name = name if '\\' not in name else name.split('\\')[-1]

        fd = self.new_folder(name)
        lst = os.listdir(path)
        folders = [f"{path}\\{f}" for f in lst if '.' not in f]
        files = [f"{path}\\{f}" for f in lst if '.' in f]

        for file in files:
            fd.upload(file)

        for folder in folders:
            fd.upload_folder(folder)

        return fd

    def get_parents(self):
        self.father = self.root.search_all.by_id(self.id, getparents=True)
        return self.father

    def move(self, some: Union[Folder, File]) -> Union[Folder, File]:
        """Move some file or folder to the current folder.

        :parameter some: File or folder to be moved to current folder.
        :return: File or Folder object that was moved.
        """

        file = self.service.files().update(fileId=some.id,
                                           addParents=self.id,
                                           removeParents=some.father,
                                           fields='id, parents, name').execute()

        if isinstance(some, File):
            return File(self, tuple([file.get('name'), file.get('id')]))
        elif isinstance(some, Folder):
            return Folder(self, tuple([file.get('name'), file.get('id')]))

    def delete(self, obj: Union[Folder, File]):
        """Deletes a folder or file from current folder.

        :parameter obj: File or Folder object to be deleted
        :return: None
        """
        self.service.files().delete(fileId=obj).execute()

    def download_all(self, path=os.getcwd(), remove=False) -> None:
        main_folder = fr"{path}\{self.name}"
        if remove and exists(main_folder):
            shutil.rmtree(main_folder)

        if not exists(main_folder):
            os.mkdir(main_folder)

        for file in self.files():
            filename = file.name if '.' in file.name else file.name+'.txt'
            if remove and exists(fr"{main_folder}\{filename}"):
                os.remove(fr"{main_folder}\{filename}")
            if not exists(fr"{main_folder}\{filename}"):
                file.download(path=main_folder)

        for folder in self.folders():
            folder.download_all(path=main_folder)

    def __list(self, mtype='all') -> Union[List[Folder], List[File]]:
        """Search in current folder for files and/or folders.

        :parameter mtype: Type of file that will be searched:
                             'folders' to gdrive folders,
                              'files' for anything other than a folder,
                              other names will search everything
        :return: List of Files or Folders objects
        """
        result = []

        if mtype == 'folders':
            mimetype = " and mimeType = 'application/vnd.google-apps.folder'"
        elif mtype == 'files':
            mimetype = " and mimeType != 'application/vnd.google-apps.folder'"
        else:
            mimetype = ''

        response = self.service.files().list(q=f"'{self.id}' in parents"+mimetype,
                                             spaces='drive',
                                             fields='files(id, name)',
                                             ).execute()
        for file in response.get('files', []):
            if mtype == 'folders':
                n_file = Folder(self, tuple([file.get('name'), file.get('id')]))
            else:
                n_file = File(self, tuple([file.get('name'), file.get('id')]))
            result.append(n_file)

        return result

    def folders(self) -> List[Folder]:
        """
        :return: List of folders that are within the current folder
        """

        self.__folders = self.__list(mtype='folders')
        return self.__folders

    def files(self) -> List[File]:
        """
        :return: List of files that are within the current folder
        """

        self.__files = self.__list(mtype='files')
        return self.__files

    def __iter__(self):
        return iter(self.__list())

    def list(self):
        return list(self)

    def search(self, name: str, ftype="folder"):
        if ftype == 'folder':
            f_list = [f for f in self.folders() if f.name == name]
            return f_list[0] if f_list else f_list
        elif ftype == 'file':
            f_list = [f for f in self.files() if f.name == name]
            return f_list[0] if f_list else f_list
        else:
            raise Exception('Invalid file type')

    def new_folder(self, name: str) -> Folder:
        """Create a new folder if it doesn't already exist

        :parameter name: Name of the folder
        :return: Folder object
        """

        check = self.__check_folder(name)
        if check and isinstance(check, Folder):
            return check
        file_metadata = {
            'name': name,
            'mimeType': 'application/vnd.google-apps.folder',
            'parents': [self.id]
        }

        file = self.service.files().create(body=file_metadata,
                                           fields='id, name'
                                           ).execute()

        return Folder(self, tuple([file.get('name'), file.get('id')]))


creds = None
service_d = None


class Root(Folder):
    def __init__(self, client=CLIENT_SECRET):
        global creds, service_d, root
        if not creds:
            creds = get_creds(client=client)
            service_d = aut_drive()
        super().__init__(None, tuple(['My drive', 'root']))
        self.search_all = Search(self)
        root = self


class Search:
    def __init__(self, root):
        self.service = service_d
        self.root = root

    def __list(self, mtype='all', ftype='', fname='', father='', fid='', trashed=False, first=False, getparents=False) \
            -> Union[List[Folder], List[File], Folder, File, None]:

        """Search in Google Drive for files and/or folders.

        :parameter mtype: Type of file that will be searched:
                             'folders' to gdrive folders,
                              'files' for anything other than a folder,
                              other names will search everything
        :return: List of Files or Folders objects
        """
        result = []
        mtype = mtype.lower()

        if mtype == 'folders' or mtype == 'folder':
            mimetype = "mimeType = 'application/vnd.google-apps.folder'"
        elif mtype == 'files' or mtype == 'file':
            mimetype = "mimeType != 'application/vnd.google-apps.folder'"
        else:
            mimetype = ''

        if ftype:
            mimetype += ' and ' if mimetype else ''
            mimetype += f"name contains '{ftype}'"
        if fname:
            mimetype += ' and ' if mimetype else ''
            mimetype += f"name contains '{fname}'"
        if father:
            mimetype += ' and ' if mimetype else ''
            mimetype += f"'{father}' in parents"

        if trashed:
            mimetype += ' and ' if mimetype else ''
            mimetype += "trashed = true"

        d = {
            'q': mimetype,
            'spaces': 'drive',
            'fields': 'files(id, name, parents, mimeType)'
        }

        d_f = {
            'q': "mimeType = 'application/vnd.google-apps.folder'",
            'spaces': 'drive',
            'fields': 'files(id, name, parents, mimeType)'
        }

        folders = self.service.files().list(**d_f).execute().get('files', [])

        if fid:
            if fid == 'root':
                return self.root
            response = self.service.files().get(fileId=fid, fields='id, name, parents, mimeType').execute()
            lst = [response]
        else:
            response = self.service.files().list(**d).execute()
            lst = response.get('files', [])

        for file in lst:
            if getparents:
                if file.get('parents'):
                    father = self.get_father(file.get('parents')[0], folders)
                else:
                    return self.root
            else:
                father = None
            if file.get('mimeType') == 'application/vnd.google-apps.folder':
                n_file = Folder(father, tuple([file.get('name'), file.get('id')]))
            else:
                n_file = File(father, tuple([file.get('name'), file.get('id')]))
            result.append(n_file)

            if first and result:
                return result[0]
            elif first:
                return None
        else:
            return result
        
    def by_name(self, name, islist=False, getparents=False):
        return self.__list(fname=name, first=not islist, mtype='', getparents=getparents)

    def by_id(self, fid, islist=False, getparents=False):
        return self.__list(fid=fid, first=not islist, mtype='', getparents=getparents)

    def get_father(self, fid, folders):

        for f in folders:
            if f.get('id') == fid:
                temp = f
                folders.remove(f)
                if temp.get('parents'):

                    return Folder(self.get_father(temp.get('parents')[0], folders),
                                  tuple([temp.get('name'), temp.get('id')]))
                else:
                    return self.root
        return self.root


root = None

if __name__ == '__main__':
    root = Root()
    t0 = time.time()
    f = root.search_all.by_name('9d160636.png', islist=True, getparents=True)
    print(f, time.time() - t0)
    f = f[0]
    while f.father:
        print(f.father)
        f = f.father

