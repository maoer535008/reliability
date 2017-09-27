# -*- mode: python -*-

block_cipher = None


a = Analysis(['main.py'],
             pathex=['D:\\myapp3'],
             binaries=[],
             hiddenimports=[],
             hookspath=[],
             runtime_hooks=[],
             excludes=[],
             win_no_prefer_redirects=False,
             win_private_assemblies=False,
             cipher=block_cipher)
pyz = PYZ(a.pure, a.zipped_data,
             cipher=block_cipher)
exe = EXE(pyz,
          a.scripts,
          a.binaries,
          a.zipfiles,	
          a.datas,
     	  [('forecast.ui','forecast.ui','DATA')],
	  [('main.ui','main.ui','DATA')],
	  [('proprocess.ui','proprocess.ui','DATA')],
          name='main',
          debug=False,
          strip=False,
          upx=True,
          console=True , icon='tubiao.ico')
