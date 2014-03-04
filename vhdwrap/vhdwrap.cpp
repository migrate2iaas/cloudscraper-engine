#define WINVER _WIN32_WINNT_WIN7
#define INITGUID
#define WIN32_LEAN_AND_MEAN
#include <Windows.h>
#include <virtdisk.h>
#include <stdlib.h>
#include <stdio.h>
#include "vhdwrap.h"

static DWORD g_dwLastError = 0;

DWORD VHDWRAP_CONVENTION GetLastVhdError()
{
	return g_dwLastError;
}

HANDLE VHDWRAP_CONVENTION CreateVhd (PCWSTR path, ULONGLONG size, BOOLEAN fixed)
{
    VIRTUAL_STORAGE_TYPE storageType =
    {
        VIRTUAL_STORAGE_TYPE_DEVICE_VHD,
        VIRTUAL_STORAGE_TYPE_VENDOR_MICROSOFT
    };

    CREATE_VIRTUAL_DISK_PARAMETERS parameters =
    {
        CREATE_VIRTUAL_DISK_VERSION_1
    };

    parameters.Version1.MaximumSize = size;
    parameters.Version1.BlockSizeInBytes = CREATE_VIRTUAL_DISK_PARAMETERS_DEFAULT_BLOCK_SIZE;
    parameters.Version1.SectorSizeInBytes = CREATE_VIRTUAL_DISK_PARAMETERS_DEFAULT_SECTOR_SIZE;
    parameters.Version1.SourcePath = NULL;

    HANDLE disk_handle = NULL;

    g_dwLastError = ::CreateVirtualDisk(&storageType,
                                path,
                                VIRTUAL_DISK_ACCESS_ALL,
                                NULL,
                                (fixed)?CREATE_VIRTUAL_DISK_FLAG_FULL_PHYSICAL_ALLOCATION : CREATE_VIRTUAL_DISK_FLAG_NONE,
                                0, // no provider-specific flags
                                &parameters,
                                NULL,
                                &disk_handle);
	return disk_handle;
}

HANDLE VHDWRAP_CONVENTION CreateExpandingVhd(PCWSTR path, ULONGLONG size)
{
    return CreateVhd(path , size, FALSE);
} 

HANDLE VHDWRAP_CONVENTION CreateFixedVhd(PCWSTR path, ULONGLONG size)
{
    return CreateVhd(path , size, TRUE);
} 



HANDLE VHDWRAP_CONVENTION OpenVhd(PCWSTR path) 
{

    VIRTUAL_STORAGE_TYPE storageType =
    {
        VIRTUAL_STORAGE_TYPE_DEVICE_VHD,
        VIRTUAL_STORAGE_TYPE_VENDOR_MICROSOFT
    };

    OPEN_VIRTUAL_DISK_PARAMETERS parameters =
    {
        OPEN_VIRTUAL_DISK_VERSION_1
    };

    parameters.Version1.RWDepth = OPEN_VIRTUAL_DISK_RW_DEPTH_DEFAULT;

	HANDLE handle = NULL;

    g_dwLastError = ::OpenVirtualDisk(&storageType,
                             path,
                             VIRTUAL_DISK_ACCESS_ALL,
                             OPEN_VIRTUAL_DISK_FLAG_NONE,
                             &parameters,
                             &handle);
	return handle;
}


BOOLEAN VHDWRAP_CONVENTION AttachVhd(HANDLE hDisk) 
{
    DWORD error = ::AttachVirtualDisk(hDisk,  NULL,
                                ATTACH_VIRTUAL_DISK_FLAG_NO_DRIVE_LETTER,
                                0, // no provider-specific flags
                                0, // no parameters
                                NULL);
	g_dwLastError = error;
	return error == ERROR_SUCCESS;
}

// returns -1 in case of errors
ULONG VHDWRAP_CONVENTION GetAttachedVhdDiskNumber(HANDLE hDisk)
{
	WCHAR diskpath[MAX_PATH] = L"";
	ULONG size = sizeof(diskpath);
	DWORD result = GetVirtualDiskPhysicalPath(hDisk, &size, diskpath);
	g_dwLastError = result;
	ULONG diskno = (ULONG)-1;
	if (result == ERROR_SUCCESS)
	{
		if (swscanf_s(diskpath , L"\\\\.\\PhysicalDrive%u" , &diskno) == 0)
		{
			if (swscanf_s(diskpath , L"\\\\.\\PHYSICALDRIVE%u" , &diskno) == 0)
			{
				swscanf_s(diskpath , L"\\\\.\\physicaldrive%u" , &diskno);
			}
		}
	}
	return diskno;
}

BOOLEAN VHDWRAP_CONVENTION DetachVhd(HANDLE hDisk)
{
	DWORD result = DetachVirtualDisk(hDisk,  DETACH_VIRTUAL_DISK_FLAG_NONE, NULL);
	g_dwLastError = result;
	return result == ERROR_SUCCESS;
}

BOOLEAN VHDWRAP_CONVENTION CloseVhd(HANDLE hDisk)
{
	return CloseHandle(hDisk) == TRUE;
}
